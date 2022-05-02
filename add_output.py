from asyncore import close_all
#from curses import COLS
from re import A
from typing import Dict, List, Set
from datetime import datetime
from pymysql import connect, cursors
import requests

EXTRA_PK_COLUMN: Dict[str,str] = {
    "column_name": "KSNAME",
    "field_name": "ksname",
    "target_type": "DbString",
    "upsolver_type": "string"
}

HOST = 'upsolver.csjjje3nufza.us-east-1.rds.amazonaws.com'
USER = 'admin'
PASSWORD = 'Upsolver1'
INFORMATION_SCHEMA = 'information_schema'


class ColInfo:
    def __init__(self, name, udt_name, target_db_type, upsolver_type, is_array, is_date, is_primary_key):
        self.name = name
        self.udt_name = udt_name
        self.target_db_type = target_db_type
        self.upsolver_type = upsolver_type
        self.is_array = is_array
        self.is_date = is_date
        self.is_primary_key = is_primary_key


class UdtInfo:
    def __init__(self, db_type, upsolver_type):
        self.db_type = db_type
        self.upsolver_type = upsolver_type


udt_bigint = UdtInfo('DbBigInt', 'number')
udt_string = UdtInfo('DbString', 'string')
udt_double = UdtInfo('DbDouble', 'number')
udt_timestamp = UdtInfo('DbTimestamp', 'number')
udt_bool = UdtInfo('DbBoolean', 'boolean')
udt_map = {'int2': udt_bigint, 'int4': udt_bigint, 'int': udt_bigint, 'int8': udt_bigint, 'bigint': udt_bigint,
           'tinyint': udt_bigint, 'double': udt_double, 'smallint': udt_bigint, 'float': udt_double,
           'varchar': udt_string, 'jsonb': udt_string, 'longtext': udt_string, 'mediumtext': udt_string,
           'enum': udt_string, 'json': udt_string,
           'decimal': udt_double,
           'bool': UdtInfo('DbBoolean', 'boolean'), 'bpchar': udt_string, 'text': udt_string,
           'date': UdtInfo('DbDate', 'number'), 'money': udt_double, 'numeric': udt_double,
           'float4': udt_double, 'float8': udt_double, 'timestamp': udt_timestamp, 'time': udt_timestamp,
           'datetime': udt_timestamp, 'bit': udt_bool,
           'timestamptz': udt_timestamp, 'char': udt_string}


def patch_request(url: str, api_token: str, json):
    response = requests.patch(url, json=json, headers=__build_auth(api_token))
    if response.status_code != 200:
        print(response.json())
    return response


def post_request(url: str, api_token: str, json):
    response = requests.post(url, json=json, headers=__build_auth(api_token))
    if response.status_code != 200:
        print(response.json())
    return response


def get_request(url: str, api_token: str):
    response = requests.get(url, headers=__build_auth(api_token))
    if response.status_code != 200:
        print(response.json())
    return response


def __build_template_url(api_prefix: str, output_id: str) -> str:
    return 'https://' + api_prefix + '.upsolver.com/template/' + output_id


def __build_auth(api_token: str) -> Dict[str, str]:
    return {'Authorization': api_token, 'Content-Type': 'application/json'}


def add_output(api_token: str, api_prefix: str, input_id: str, full_table_name: str, logical_delete_col: str,
               workspaces: List[str], columns_file_path: str, use_upsolver_primary_key_column: bool):
    """
    Create an output for a specified table.

    :param api_token: The API token used to authenticate with the Upsolver API
    :param api_prefix: The prefix used for this api instance. Should be 'api' or 'api-API_GUID'
    :param input_id: The id of the Upsolver output or data source to read from. This should be an output
                     without any transformations on top of a CDC data source
    :param full_table_name: The full name of the table, including the schema. This should match the data.full_table_name
                            field contents
    :param logical_delete_col: The column name to use if we're doing logical deletes. Set to '' otherwise
    :param workspaces: The list of workspaces to attach to the generated output
    :param columns_file_path: Path to the file that contains the column metadata. The file should contain a csv with triples
                              full_table_name,column_name,udt_name,is_primary_key
    :param use_upsolver_primary_key_column Add an UPSOLVER_PRIMARY_KEY column to each table, instead of using the normal
                                           columns as primary keys.
    """
    # create blank output
    print("Creating a snowflake output for " + full_table_name)
    response = post_request(__build_template_url(api_prefix, ''), api_token,
                            json={"clazz": "OutputTemplateRequest",
                                  "displayData": {"name": full_table_name, "description": ""},
                                  "inputs": [input_id],
                                  "outputParameters": {"clazz": "SnowflakeOutputParameters",
                                                       "shouldFlatten": False,
                                                       "failOnWriteError": True,
                                                       "selectedOutputType": "SnowflakeOutputParameters",
                                                       "createTable": True}, "groupBy": False,
                                  "workspaces": workspaces})
    data = response.json()

    # load db info and add columns based on postgres
    output_id = data['id']
    template_url = __build_template_url(api_prefix, output_id)
    __filter_table(api_prefix, output_id, full_table_name, api_token)
    info: List[ColInfo] = __get_col_info_for_table(full_table_name, columns_file_path)
    for col in info:
        get_first_field_time(api_prefix, input_id, full_table_name, 'data.row.' + col.name, col.upsolver_type,
                             api_token) # TODO: use minimum last_seen to set the Output start time
        add_new_column(api_prefix, output_id, col.name, col.name, col.target_db_type, col.is_array, col.upsolver_type,
                       col.is_primary_key and not use_upsolver_primary_key_column, api_token)

    if use_upsolver_primary_key_column:
        primary_key_column = "upsolver_primary_key"
        __add_column(api_prefix, output_id, primary_key_column, "DbString", False, api_token)
        __map_column(api_prefix, output_id, "data.primary_key", primary_key_column, 'string', False, api_token)
        set_upsert_key(api_prefix, output_id, primary_key_column, api_token)

    delete_field_name = "data.metadata.is_delete"
    if logical_delete_col != '':
        __add_column(api_prefix, output_id, logical_delete_col, 'DbBoolean', False, api_token)
        __map_column(api_prefix, output_id, delete_field_name, logical_delete_col, 'boolean', False, api_token)
    else:
        patch_request(template_url, api_token,
                      json={"clazz": "SetIsDelete",
                            "field": {"label": delete_field_name, "name": delete_field_name,
                                      "nativeType": {"label": "boolean", "subFields": [],
                                                     "name": "boolean", "isArray": False},
                                      "key": delete_field_name + "-boolean",
                                      "lastLabel": delete_field_name, "operationId": "input",
                                      "featureField": False}})

    return output_id

def check_and_update_output(api_token: str, api_prefix: str, input_id: str, full_table_name: str, logical_delete_col: str,
               workspaces: List[str], columns_file_path: str, use_upsolver_primary_key_column: bool):
    """
    Check an output for a specified table and return output_id, yes/no if output updated, rerun time (earliest field)

    :param api_token: The API token used to authenticate with the Upsolver API
    :param api_prefix: The prefix used for this api instance. Should be 'api' or 'api-API_GUID'
    :param input_id: The id of the Upsolver output or data source to read from. This should be an output
                     without any transformations on top of a CDC data source
    :param full_table_name: The full name of the table, including the schema. This should match the data.full_table_name
                            field contents
    :param logical_delete_col: The column name to use if we're doing logical deletes. Set to '' otherwise
    :param workspaces: The list of workspaces to attach to the generated output
    :param columns_file_path: Path to the file that contains the column metadata. The file should contain a csv with triples
                              full_table_name,column_name,udt_name,is_primary_key
    :param use_upsolver_primary_key_column Add an UPSOLVER_PRIMARY_KEY column to each table, instead of using the normal
                                           columns as primary keys.
    """
    # query existing outputs based on full_table_name
    print("Retrieve output for " + full_table_name)
    output_url: str = 'https://' + api_prefix + '.upsolver.com/template/?clazz=OutputTemplate'

    response = get_request(output_url, api_token)
    output_data: List[Dict[str, any]] = response.json()
    output = next(
        item for item in output_data if
        item["displayData"]['name'] == full_table_name and item["status"]['clazz'] == 'Running')

    output_id: str = output['id']

    info: List[ColInfo] = __get_col_info_for_table(full_table_name, columns_file_path)
    earliestRunTime = None
    outputCols = getExistingOutputCols(api_token,api_prefix,output_id)
    isMissingColumns = False
    for col in info:
        if col.name.upper() not in outputCols:

            first_seen = get_first_field_time(api_prefix, input_id, full_table_name, 'data.row.' + col.name, col.upsolver_type,
                             api_token) 
            edit_url = 'https://' + api_prefix + '.upsolver.com/template/edit/' + output_id
            print("Editing the output")
            response = post_request(edit_url, api_token, None)
            data = response.json()
            edit_output_id = data['id']
            add_new_column(api_prefix, edit_output_id, col.name, col.name, col.target_db_type, col.is_array, col.upsolver_type,
                       False, api_token)
            
            if earliestRunTime is None or first_seen < earliestRunTime:
                earliestRunTime = first_seen
            isMissingColumns = True


    return edit_output_id,isMissingColumns,earliestRunTime


def getExistingOutputCols(api_token,api_prefix,output_id):

    col_ouput_url: str = 'https://' + api_prefix + '.upsolver.com/inspection/output/inspect/fields/v2/' + output_id 

    print("Getting existing col list from output")
    response = get_request(col_ouput_url, api_token)
    data = response.json()
    cols = []
    for fields in data["fields"]:
        cols.append(fields["field"]['name'])
    return cols



def set_upsert_key(api_prefix: str, output_id: str, field_name: str, api_token: str):
    print("Setting upsert key: " + field_name)
    patch_request(__build_template_url(api_prefix, output_id), api_token,
                  json={"clazz": "ToggleUpsertKey", "name": field_name.upper()})



def add_column_to_existing_output(api_prefix: str, output_id: str, column_name: str,
                                  target_type: str, is_array: bool, upsolver_type: str, api_token: str):
    response = get_request(__build_template_url(api_prefix, output_id), api_token)
    output = response.json()

    input_id = output['inputs'][0]
    output_interval = output["outputInterval"]
    compute_environment = output["computeEnvironment"]

    if is_array:
        field_type = upsolver_type + '[]'
    else:
        field_type = upsolver_type

    first_seen = get_first_field_time(api_prefix, input_id, output["displayData"]["name"],
                                      'data.row.' + column_name, field_type, api_token)

    edit_url = 'https://' + api_prefix + '.upsolver.com/template/edit/' + output_id
    print("Editing the output")
    response = post_request(edit_url, api_token, None)
    data = response.json()
    edit_output_id = data['id']
    add_new_column(api_prefix, edit_output_id, column_name, column_name,
                   target_type, is_array, upsolver_type, False, api_token)

    deploy_url = 'https://' + api_prefix + '.upsolver.com/template/deploy/' + edit_output_id + '?force=true'
    print("Redeploying the output")
    post_request(deploy_url, api_token, json={"clazz": "OutputTemplateDeployParameters",
                                              "computeEnvironment": compute_environment,
                                              "outputInterval": output_interval,
                                              "startExecutionFrom": {"clazz": "AtTime", "time": first_seen},
                                              "run": True})


def get_first_field_time(api_prefix: str, input_id: str, full_table_name: str,
                         field_name: str, field_type: str, api_token: str) -> datetime:
    distr_url = 'https://' + api_prefix + '.upsolver.com/inspection/input/field/' + input_id + '/' + field_name + \
                '/' + field_type + '?onlyDistributions=true&distribution.take=0&eventType=' + full_table_name
    print("Getting field start time for " + field_name)
    response = get_request(distr_url, api_token)
    data = response.json()
    first_seen = datetime.strptime(data["seen"]["first"] if data.get("seen") else data.get("lastUpdate"),
                                   '%Y-%m-%dT%H:%M:%S%z') # TODO: fails for columns with no data. We should use get() or something similar
    print("First seen time for " + field_name + ": " + first_seen.strftime('%Y-%m-%dT%H:%M:%S%z'))
    return first_seen


def __return_field_details(name: str, udt_type: str, is_primary_key: bool) -> ColInfo:
    is_array = udt_type[0] == '_'
    if is_array:
        udt_type = udt_type[1:]
    if udt_type in udt_map:
        udt = udt_map[udt_type]
    else:
        print("No mapping for udt_type " + udt_type)
        udt = udt_string
    return ColInfo(name, udt_type, udt.db_type, udt.upsolver_type, is_array, udt.db_type == 'DbDate', is_primary_key)


def add_new_column(api_prefix: str, output_id: str, column_name: str, field_name: str,
                   target_type: str, is_array: bool, upsolver_type: str, is_primary_key: bool, api_token: str):
    __add_column(api_prefix, output_id, column_name, target_type, is_array, api_token)
    __map_column(api_prefix, output_id, field_name, column_name, upsolver_type, is_array, api_token)
    if is_primary_key:
        set_upsert_key(api_prefix, output_id, column_name, api_token)


def __add_column(api_prefix: str, output_id: str, name: str, db_type: str, is_array: bool, api_token: str):
    print("Adding column " + name)
    if is_array:
        actual_db_type = 'DbVariant'
    else:
        actual_db_type = db_type

    response = patch_request(__build_template_url(api_prefix, output_id), api_token,
                             json={"clazz": "SnowflakeAddDatabaseColumn", "column": name.upper(),
                                   "columnType": {"clazz": actual_db_type}, "nullable": True})
    data = response.json()
    if response.status_code != 200:
        print(data)


def __get_col_info_for_table(table, columns_file_path) -> List[ColInfo]:
    # conn = psycopg2.connect(database=pgDb,user=pgUser,password=pgPwd,host=pgHost,port=pgPort)
    # sql = "SELECT table_name, column_name,udt_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'"
    # cur = conn.cursor()
    # cur.execute(sql)
    schema = table.split('.')[0]
    conn = connect(host=HOST,
                   user=USER,
                   password=PASSWORD,
                   database=INFORMATION_SCHEMA,
                   cursorclass=cursors.DictCursor)
    sql = f"SELECT CONCAT(table_schema, '.', table_name) as table_name, column_name, data_type, CASE WHEN column_key = 'PRI' THEN 'true' else 'false' end as is_primary_key FROM information_schema.columns c WHERE TABLE_SCHEMA = '{schema}' and CONCAT(table_schema, '.', table_name) = '{table}' ORDER BY ordinal_position"
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            cur = cursor.fetchall()

    # cur = open(columns_file_path).readlines()
    table_columns = {}
    for row in cur:
        # col = row.strip().split(',')
        found_table = row["table_name"]
        if found_table == table:
            name = row["COLUMN_NAME"]
            udt_name = row["DATA_TYPE"]
            is_primary_key = row["is_primary_key"].lower() == 'true'
            if table not in table_columns:
                table_columns[table] = []
            col_dict: ColInfo = __return_field_details(name, udt_name, is_primary_key)
            table_columns[table].append(col_dict)
    # conn.close()
    return table_columns[table]


def __map_column(api_prefix: str, output_id: str, name: str, column_name: str, upsolver_type: str, is_array: bool,
                 api_token: str):
    template_url = __build_template_url(api_prefix, output_id)

    if column_name != name:
        real_name = name
    else:
        real_name = 'data.row.' + name

    if is_array:
        real_name = real_name + '[]'

    print("Mapping column " + real_name)
    patch_request(template_url, api_token,
                  json={"clazz": "AddRawField", "field": {"name": real_name,
                                                          "nativeType": {"name": upsolver_type,
                                                                         "isArray": False}},
                        "name": column_name.upper()})


def __filter_table(api_prefix: str, output_id: str, full_table_name: str, api_token: str):
    full_table_name_field = 'data.full_table_name'
    patch_request(__build_template_url(api_prefix, output_id), api_token,
                  json={"clazz": "AddCustomField",
                        "feature": {"clazz": "PartialFeatureDefinition",
                                    "feature": {"clazz": "AnyFeatures$EqualToFilterV2"},
                                    "featureInputs": [{"clazz": "FieldFeatureInput",
                                                       "field": {"label": full_table_name_field,
                                                                 "name": full_table_name_field,
                                                                 "nativeType": {
                                                                     "label": "string",
                                                                     "subFields": [],
                                                                     "name": "string",
                                                                     "isArray": False},
                                                                 "key": full_table_name_field + "-string",
                                                                 "lastLabel": full_table_name_field,
                                                                 "operationId": "input",
                                                                 "featureField": False,
                                                                 "inputs": [{
                                                                     "id": "e4719cec-5398-4de2-bb0b-1c22b56653d7",
                                                                     "state": {
                                                                         "clazz": "FoundInData"}}]},
                                                       "inputName": "first"},
                                                      {"clazz": "LiteralFeatureInput",
                                                       "nativeType": {"name": "string",
                                                                      "isArray": False},
                                                       "literal": full_table_name,
                                                       "inputName": "second"}],
                                    "filterParameters": {"enabled": True, "inverted": False},
                                    "inline": True, "infix": True,
                                    "field": {"label": "inline_custom_field",
                                              "name": "inline_custom_field",
                                              "nativeType": {"label": "boolean",
                                                             "subFields": [],
                                                             "name": "boolean",
                                                             "isArray": False},
                                              "key": "inline_custom_field-boolean",
                                              "lastLabel": "inline_custom_field",
                                              "operationId": "", "featureField": True,
                                              "clazz": "InlineField"}},
                        "context": "calculated", "map": False})


def run_output(api_prefix: str, output_id: str, table_name: str, jdbc_connection: str,
               schema_catalog: str, schema_name: str, cloud_storage_connection: str, api_token: str,
               compute_environment: str, output_interval: int):
    """
    Run an output that was already configured.

    :param api_prefix: The prefix used for this api instance. Should be 'api' or 'api-API_GUID'
    :param output_id:            The id of the Snowflake output to run
    :param table_name:           The name of the table to create in snowflake
    :param jdbc_connection:      The Upsolver connection id to Snowflake
    :param schema_catalog:       The catalog to use in Snowflake
    :param schema_name:          The schema to use in Snowflake
    :param cloud_storage_connection: The connection to use to store intermediate files which will be copied to Snowflake
    :param api_token:            The API token used to authenticate with the Upsolver API
    :param compute_environment:  The cluster id that will run the output
    :param output_interval:      The interval in which to write data to Snowflake in minutes
    """

    template_url = __build_template_url(api_prefix, output_id)

    # prepare for output
    print("Setting Snowflake catalog information")
    response = patch_request(template_url, api_token,
                             json={"clazz": "SetMandatoryOutputParameters",
                                   "mandatory": {"clazz": "MandatorySnowflakeOutputParameters",
                                                 "jdbcConnection": jdbc_connection,
                                                 "schema": {"catalog": schema_catalog,
                                                            "name": schema_name},
                                                 "tableName": table_name.upper(),
                                                 "cloudStorageConnection": cloud_storage_connection}})
    if response.status_code == 200:
        # deploy
        deploy_url = __build_template_url(api_prefix, 'deploy/' + output_id + "?force=true")
        start_class = 'InputStart'
        print("Deploying Snowflake output")
        post_request(deploy_url, api_token,
                     json={"clazz": "OutputTemplateDeployParameters",
                           "computeEnvironment": compute_environment,
                           "outputInterval": output_interval,
                           "startExecutionFrom": {"clazz": start_class}, "run": True})

                           
    else:
        print(response.json())

        



def add_new_tables(api_token: str, api_prefix: str, cdc_data_source_name: str, snowflake_connection_name: str,
                   snowflake_catalog: str, snowflake_schema: str, cloud_storage_connection_name: str,
                   compute_cluster_name: str, columns_file_path: str, output_interval: int, logical_deletes: bool,
                   tables_include_list: Set[str], tables_exclude_list: Set[str]):
    """
    Create and run outputs for all the tables that exist in the CDC data source, but aren't being written to Snowflake.

    :param api_token: The API token used to authenticate with the Upsolver API
    :param api_prefix: The prefix of the specific API server. Should be either "api", "api-GUID" or "api-private-GUID"
    :param cdc_data_source_name: The CDC data source name to read data from
    :param snowflake_connection_name: The name of the Snowflake connection to use to write data
    :param snowflake_catalog: The name of the catalog in Snowflake to write to
    :param snowflake_schema: The name of the schema in Snowflake to write to
    :param cloud_storage_connection_name: The name of the cloud storage connection to use as a staging bucket
    :param compute_cluster_name: The name of the compute cluster the created outputs should run on
    :param columns_file_path: The local path to the file that contains the list of columns per table. This file should
                              be in the format full_table_name,column_name,column_type,is_primary_key
    :param output_interval: The amount of minutes between writes to Snowflake
    :param logical_deletes: If the tables created should actually delete based on deletes in the source, or just add
                            an is_deleted column
    :param tables_include_list: A set of full table names to include, out of the list of all the tables in the source.
    :param tables_exclude_list: A set of full table names to exclude.
    """

    output_url: str = 'https://' + api_prefix + '.upsolver.com/template/?clazz=OutputTemplate'
    input_url: str = 'https://' + api_prefix + '.upsolver.com/inputs/'
    conn_url: str = 'https://' + api_prefix + '.upsolver.com/connections/uses' # TODO: replace url to connections/ and remove permissions to lookup-table:view:list
    dashboard_url: str = 'https://' + api_prefix + '.upsolver.com/environments/dashboard'

    full_table_name_field: str = "data.full_table_name"

    print("Getting the compute environment id")
    response = get_request(dashboard_url, api_token)
    data: List[Dict[str, any]] = response.json()
    compute_environment: str = \
        next(item for item in data if item['environment']['environmentType'] == 'compute' and
             item['environment']['displayData']['name'] == compute_cluster_name)['environment']['id']

    print("Getting connection ids")
    response = get_request(conn_url, api_token)
    data = response.json()
    item = next(item for item in data if
                item['info']['connection']['clazz'] == 'SnowflakeConnection' and
                item['info']['connection']['displayData']['name'] == snowflake_connection_name)
    jdbc_connection = item['info']['id']
    print("jdbcConnection: " + jdbc_connection)

    item = next(item for item in data if
                item['info']['connection']['clazz'] == 'S3Connection' and item['info']['connection'][
                    'displayData']['name'] == cloud_storage_connection_name)
    cloud_storage_connection = item['info']['id']
    print("cloudStorageConnection: " + cloud_storage_connection)

    # get S3 bucket for athena output
    print("Getting outputs")
    response = get_request(output_url, api_token)
    output_data: List[Dict[str, any]] = response.json()
    existing_outputs: Set[str] = {item["displayData"]['name'] for item in output_data if
                                  item["isRunning"] or not item["deployedEver"]}

    response = get_request(input_url, api_token)
    input_data: List[Dict[str, any]] = response.json()
    output = next(
        item for item in input_data if
        item["displayData"]['name'] == cdc_data_source_name and item["status"]['clazz'] == 'Running')
    ds_id: str = output['id']
    distr_url: str = 'https://' + api_prefix + '.upsolver.com/inspection/input/field/' + ds_id + '/' + \
                     full_table_name_field + '/string?onlyDistributions=true&distribution.take=100000'

    # copy the workspaces from the output
    workspaces = output['workspaces']

    # query to see if any new tables
    print("Getting table list from value distribution")
    response = get_request(distr_url, api_token)
    data: Dict[str, any] = response.json()
    distr = (data['distribution']) # TODO: Only creates outputs that have data. So new tables without data won't be created. Should we change this ?
    if logical_deletes:
        delete_key = 'is_deleted'
    else:
        delete_key = ''

    for table in distr:
        full_table_name: str = table['value']

       


        if full_table_name in existing_outputs or full_table_name in tables_exclude_list or \
                (len(tables_include_list) > 0 and full_table_name not in tables_include_list):
            print(full_table_name + " will not be processed. It either already exists, is in the exclude list, " +
                  "or isn't in the include list")
        else:
            output_id = add_output(api_token, api_prefix, ds_id, full_table_name,
                                   delete_key, workspaces, columns_file_path, False)
            if EXTRA_PK_COLUMN.get("column_name"):
                add_new_column(api_prefix=api_prefix, output_id=output_id, is_array=False, is_primary_key=True,
                               api_token=api_token, **EXTRA_PK_COLUMN)
            print("output: " + output_id)
            # run output
            table_name = full_table_name.split('.')[-1]
            run_output(api_prefix, output_id, table_name, jdbc_connection,
                       snowflake_catalog, snowflake_schema, cloud_storage_connection, api_token,
                       compute_environment, output_interval)


def update_existing_tables(api_token: str, api_prefix: str, cdc_data_source_name: str, snowflake_connection_name: str,
                   snowflake_catalog: str, snowflake_schema: str, cloud_storage_connection_name: str,
                   compute_cluster_name: str, columns_file_path: str, output_interval: int, logical_deletes: bool,
                   tables_include_list: Set[str], tables_exclude_list: Set[str]):
    """
    Check all the tables that exist in the CDC data source, and if there are new fields that dont exist in SnowFlake add
    to existing output and rerun.

    :param api_token: The API token used to authenticate with the Upsolver API
    :param api_prefix: The prefix of the specific API server. Should be either "api", "api-GUID" or "api-private-GUID"
    :param cdc_data_source_name: The CDC data source name to read data from
    :param snowflake_connection_name: The name of the Snowflake connection to use to write data
    :param snowflake_catalog: The name of the catalog in Snowflake to write to
    :param snowflake_schema: The name of the schema in Snowflake to write to
    :param cloud_storage_connection_name: The name of the cloud storage connection to use as a staging bucket
    :param compute_cluster_name: The name of the compute cluster the created outputs should run on
    :param columns_file_path: The local path to the file that contains the list of columns per table. This file should
                              be in the format full_table_name,column_name,column_type,is_primary_key
    :param output_interval: The amount of minutes between writes to Snowflake
    :param logical_deletes: If the tables created should actually delete based on deletes in the source, or just add
                            an is_deleted column
    :param tables_include_list: A set of full table names to include, out of the list of all the tables in the source.
    :param tables_exclude_list: A set of full table names to exclude.
    """

    output_url: str = 'https://' + api_prefix + '.upsolver.com/template/?clazz=OutputTemplate'
    input_url: str = 'https://' + api_prefix + '.upsolver.com/inputs/'
    conn_url: str = 'https://' + api_prefix + '.upsolver.com/connections/uses' # TODO: replace url to connections/ and remove permissions to lookup-table:view:list
    dashboard_url: str = 'https://' + api_prefix + '.upsolver.com/environments/dashboard'

    full_table_name_field: str = "data.full_table_name"

    print("Getting the compute environment id")
    response = get_request(dashboard_url, api_token)
    data: List[Dict[str, any]] = response.json()
    compute_environment: str = \
        next(item for item in data if item['environment']['environmentType'] == 'compute' and
             item['environment']['displayData']['name'] == compute_cluster_name)['environment']['id']

    print("Getting connection ids")
    response = get_request(conn_url, api_token)
    data = response.json()
    item = next(item for item in data if
                item['info']['connection']['clazz'] == 'SnowflakeConnection' and
                item['info']['connection']['displayData']['name'] == snowflake_connection_name)
    jdbc_connection = item['info']['id']
    print("jdbcConnection: " + jdbc_connection)

    item = next(item for item in data if
                item['info']['connection']['clazz'] == 'S3Connection' and item['info']['connection'][
                    'displayData']['name'] == cloud_storage_connection_name)
    cloud_storage_connection = item['info']['id']
    print("cloudStorageConnection: " + cloud_storage_connection)

    # get S3 bucket for athena output
    print("Getting outputs")
    response = get_request(output_url, api_token)
    output_data: List[Dict[str, any]] = response.json()
    existing_outputs: Set[str] = {item["displayData"]['name'] for item in output_data if
                                  item["isRunning"] or not item["deployedEver"]}

    response = get_request(input_url, api_token)
    input_data: List[Dict[str, any]] = response.json()
    output = next(
        item for item in input_data if
        item["displayData"]['name'] == cdc_data_source_name and item["status"]['clazz'] == 'Running')
    ds_id: str = output['id']
    distr_url: str = 'https://' + api_prefix + '.upsolver.com/inspection/input/field/' + ds_id + '/' + \
                     full_table_name_field + '/string?onlyDistributions=true&distribution.take=100000'

    # copy the workspaces from the output
    workspaces = output['workspaces']

    # query to see if any new tables
    print("Getting table list from value distribution")
    response = get_request(distr_url, api_token)
    data: Dict[str, any] = response.json()
    distr = (data['distribution']) # TODO: Only creates outputs that have data. So new tables without data won't be created. Should we change this ?
    if logical_deletes:
        delete_key = 'is_deleted'
    else:
        delete_key = ''

    for table in distr:
        full_table_name: str = table['value']

        if full_table_name in existing_outputs :
            print(full_table_name + " will be checked for existing columns.")
            result= check_and_update_output(api_token, api_prefix, ds_id, full_table_name,
                                   delete_key, workspaces, columns_file_path, False)
            output_id = result[0]
            rerun = result[1]
            first_seen = result[2].strftime("%Y-%m-%dT%H:%M:%SZ")
            print("output: " + output_id + " rerun: " + str(rerun) + " first_seen: " + first_seen)
            # run output if rerun is true
            if rerun:
                deploy_url = 'https://' + api_prefix + '.upsolver.com/template/deploy/' + output_id + '?force=true'
    
                post_request(deploy_url, api_token, json={"clazz": "OutputTemplateDeployParameters",
                                              "computeEnvironment": compute_environment,
                                              "outputInterval": output_interval,
                                              "startExecutionFrom": {"clazz": "AtTime", "time": first_seen},
                                              "run": True})


