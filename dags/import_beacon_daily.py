# coding=utf-8
# pylint: disable=broad-except
# Copyright (c) 2016-2018, F5 Networks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This module contains F5 Beacon daily importer DAG for BigQuery
"""

from datetime import timedelta
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.f5_beacon_plugin import F5BeaconMetricQueryDailyExporterOperator
from airflow.hooks.f5_beacon_plugin import BeaconHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': 'j.gruber@f5.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'import_beacon_daily',
    default_args=default_args,
    description='DAG to import dailies from Beacon Query API Output',
    schedule_interval=timedelta(days=1)
)

beacon_conn_id='beacon_import_f5cs_conn'
destination_dir='/home/airflow/gcs/data'

t1 = F5BeaconMetricQueryDailyExporterOperator(
    task_id='export_metrics_from_beacon',
    beacon_conn_id=beacon_conn_id,
    destination_dir=destination_dir,
    dag=dag
)

beacon_hook = BeaconHook(beacon_conn_id)
conn = beacon_hook.get_conn()
account_id = conn.extra_dejson['account_id']
dataset_name = "f5_metric_%s" % account_id.lower().replace('-','_')
table_name = "beacon_metrics"
metric_file = "./line_metrics.json" 
schem_file = "./line_schema.json"

assure_dataset_command = """
cd {{ params.destination_dir }}/{{ run_id }}
is_dataset=$(bq ls | grep {{ params.dataset_name }}| wc -l)
if [ "$is_dataset" != "1" ]
then
    bq mk {{ params.dataset_name }}
fi
"""

t2 = BashOperator(
    task_id='assure_dataset',
    bash_command=assure_dataset_command,
    params={'destination_dir': destination_dir, 'dataset_name': dataset_name},
    dag=dag
)

assure_table_command = """
cd {{ params.destination_dir }}/{{ run_id }}
is_table=$(bq ls {{ params.dataset_name }}|grep {{ params.table_name }}| wc -l)
if [ "$is_table" != "1" ]
then
    bq mk --table --schema {{ params.schema_file }} --time_partitioning_field ts {{ params.dataset_name }}.{{ params.table_name }}
fi
"""

t3 = BashOperator(
    task_id='assure_table',
    bash_command=assure_table_command,
    params={'destination_dir': destination_dir, 'dataset_name': dataset_name, 'table_name': table_name, 'schema_file': schem_file },
    dag=dag
)

import_command = """
cd {{ params.destination_dir }}/{{ run_id }}
bq load --source_format=NEWLINE_DELIMITED_JSON {{ params.dataset_name }}.{{ params.table_name }} {{ params.metric_file }} {{ params.schema_file }}
"""

t4 = BashOperator(
    task_id='import_data',
    bash_command=import_command,
    params={'destination_dir': destination_dir, 'dataset_name': dataset_name, 'table_name': table_name, 'metric_file': metric_file, 'schema_file': schem_file},
    dag=dag
)

cleanup_command="""
rm -rf {{ params.destination_dir }}/{{ run_id }}
"""

t5 = BashOperator(
    task_id='clean_up',
    trigger_rule=TriggerRule.ALL_DONE,
    bash_command=cleanup_command,
    params={'destination_dir': destination_dir},
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5