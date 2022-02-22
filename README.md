
# Snowflake CDC

Leverage this script to automate tasks related to CDC (change data capture) and database replication writing to Snowflake with Upsolver. For more information, read about [CDC with Upsolver](https://www.upsolver.com/blog/mysql-cdc-and-database-replication-for-the-data-lake-age).

Note: this script is best suited for bulk changes that would be too time-intensive to update across all the dependencies. If you have one-off changes you wish to make, like adding a single column to a target Snowflake table, those may be easier achieved by using the Upsolver user interface.

## Process Summary

The general flow of Upsolver is described below:
 
 1. Ingest CDC database(s) into Upsolver, with raw data stored on Amazon S3 or Azure Blob
 2. Create ETL pipelines in the form of Upsolver outputs from the CDC data source by table.
 3. Create ETL pipelines to write to Snowflake for each underlying table, represented with different table schemas according to the desired logical delete strategy.

The first item above is to be completed in the Upsolver interface. For more information on configuring your CDC data source, see [the docs](https://docs.upsolver.com/upsolver-1/connecting-data-sources/cdc-data-sources-debezium).

The next items in the flow can be executed via the `add_output()` script and its contained functions. Item 2 will be represented by a SQL pipeline similar to:
``` SQL
SELECT actual_full_table_name AS full_table_name:STRING,
       data.row.* AS row_*,
       data.old_row.* AS old_row_*,
       data.metadata.is_delete AS is_delete,
       data.primary_key AS primary_key
  FROM "My CDC Data Source"
      WHERE actual_full_table_name = 'my_database.target_table'

```
The **actual_full_table_name** is defined as a calculated field concatenating the original database name and the table name, such as: 
```SQL 
SET actual_full_table_name = STRING_FORMAT('{0}.{1}', data.database_name, REGEXP_REPLACE(data.table_name, '_part_\d+$', ''));
```

This ETL transformation will produce a dynamic schema of columns prepended by `old_row_` and `row_` to distinguish previous and current values.

The 3rd and final step will leverage this Upsolver output and write to a Snowflake table based on the columns file.

## Setup your workspace

 * API Token: details on how to generate [here](https://docs.upsolver.com/upsolver-1/guide-for-developers/upsolver-rest-api)
 * API Prefix: The prefix of the specific API server to inter. Should be either "api", "api-GUID" or "api-private-GUID"
 * Columns file: A path to a file containing metadata. This file should contain a CSV with triples **full_table_name**, **column_name**, **udt_name** and **is_primary_key**
 * Input ID: The resource ID of the CDC data source


## Common Functions

### `add_output`

Create an output for a specified table in the CDC source. This will create the outputs described in steps 2 and 3 of the [process summary](#process-summary).

#### Parameter List

| parameter name                  | description |
| ------------------------------- | ----------- |
| api_token                       | The API token used to authenticate with the Upsolver API |
| api_prefix                      | The prefix used for this api instance. Should be 'api' or 'api-API_GUID' |
| input_id                        | The id of the Upsolver output or data source to read from. This should be an output without any transformations on top of a CDC data source |
| full_table_name                 | The full name of the table, including the schema. This should match the data.full_table_name field contents | 
| logical_delete_col              | The column name to use if we're doing logical deletes. Set to '' otherwise |
| workspaces                      | The list of workspaces to attach to the generated output |
| columns_file_path               | Path to the file that contains the column metadata. The file should contain a csv with triples full_table_name,column_name,udt_name,is_primary_key |
| use_upsolver_primary_key_column | dd an UPSOLVER_PRIMARY_KEY column to each table, instead of using the normal columns as primary keys.|


### `run_output`

Run an output that was already configured. This deploys the output that was added manually or by the **add_output** function.

#### Parameter List

| parameter name | description |
| -------------- | ----------- |
| api_prefix |           The prefix used for this api instance. Should be 'api' or 'api-API_GUID'|
| api_token |            The API token used to authenticate with the Upsolver API|
| output_id |            The id of the Snowflake output to run|
| table_name |           The name of the table to create in snowflake|
| jdbc_connection |      The Upsolver connection id to Snowflake|
| schema_catalog |       The catalog to use in Snowflake|
| schema_name |          The schema to use in Snowflake|
| cloud_storage_connection | The connection to use to store intermediate files which will be copied to Snowflake|
| compute_environment |  The cluster id that will run the output|
| output_interval |      The interval in which to write data to Snowflake in minutes|


### `add_new_tables`

Create and run outputs for all the tables that exist in the CDC data source, but aren't being written to Snowflake.

| parameter name | description |
| -------------- | ----------- |
| api_token | The API token used to authenticate with the Upsolver API |
| api_prefix | The prefix of the specific API server. Should be either "api", "api-GUID" or "api-private-GUID" |
| cdc_data_source_name | The CDC data source name to read data from |
| snowflake_connection_name | The name of the Snowflake connection to use to write data |
| snowflake_catalog | The name of the catalog in Snowflake to write to |
| snowflake_schema | The name of the schema in Snowflake to write to |
| cloud_storage_connection_name | The name of the cloud storage connection to use as a staging bucket |
| compute_cluster_name | The name of the compute cluster the created outputs should run on |
| columns_file_path | The local path to the file that contains the list of columns per table. |
| output_interval | The amount of minutes between writes to Snowflake |
| logical_deletes | If the tables created should actually delete based on deletes in the source, or just add an is_deleted column |
| tables_include_list | A set of full table names to include, out of the list of all the tables in the source. |
| tables_exclude_list | A set of full table names to exclude. |