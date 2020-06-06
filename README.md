# beacon-airflow

F5 Beacon Airflow Components

This Airflow plugin makes HTTP REST queries against the F5 Beacon Metric API extracting metric data for a specified account.

You will need to create a connection with the following information:

| Attribute | Value       | Description                                |
| --------- | ----------- | ------------------------------------------ |
| Conn Id   | String      | user defined connection id                 |
| Conn Type | String      | File (path)                                |
| Host      | String      | api.cloudservices.f5.com                   |
| Schema    | String      | v1                                         |
| Login     | String      | F5 Beacon user id                          |
| Password  | String      | F5 Beacon user password                    |
| Extra     | JSON String | { "account_id" : "[F5 Beacon Account ID]"} |

Then you will want to create a DAG utilizing the Operators in this plugin.

There is a sample DAG in the `dags` directory of this repository.

There ware two operators supplied by this plugin:

`airflow.operators.f5_beacon_plugin.F5BeaconMetricQueryExporterOperator`

This operator will extract metrics for the specified account between unix timestamps defined as `start_timestamp` and `stop_timestamp`. The input arguments for this operator are:

| Argument        | Value  | Description                                                                                                  |
| --------------- | ------ | ------------------------------------------------------------------------------------------------------------ |
| beacon_conn_id  | String | user defined connection id (see above)                                                                       |
| destination_dir | String | destination director to export JSON files, the default is `/home/airflow/gcs/data` for Google Cloud Composer |
| start_timestamp | int    | the staring timestamp to extact metrics                                                                      |
| stop_timestamp  | int    | the stopping timestamp to extract metrics                                                                    |

The output of this operator will be a folder named for the DAG `run_id` containing a new line delimited JSON file `line_metrics.json` and a JSON schema file `schema`. Both files will reside in the specified `destination_dir`.

`airflow.operators.f5_beacon_plugin.F5BeaconMetricQueryDailyExporterOperator`

This operator will extract metrics for the day specified by the DAG `execution_date`. The input arguments for this operator are:

| Argument        | Value  | Description                                                                                                  |
| --------------- | ------ | ------------------------------------------------------------------------------------------------------------ |
| beacon_conn_id  | String | user defined connection id (see above)                                                                       |
| destination_dir | String | destination director to export JSON files, the default is `/home/airflow/gcs/data` for Google Cloud Composer |

The output of this operator will be a folder named for the DAG `run_id` containing a new line delimited JSON file `line_metrics.json` and a JSON schema file `schema`. Both files will reside in the specified `destination_dir`.

You can then defined other tasks in your DAG which use Big Query import operations to build your dataset and tables.

The supplied sample DAG is designed to work in Google Cloud Composer and will create a BigQuery dataset and tables from your Beacon metric data.
