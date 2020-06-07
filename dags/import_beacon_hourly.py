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

from airflow.operators.f5_beacon_plugin import F5BeaconMetricQueryHourlyExporterOperator
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
    'import_beacon_hourly',
    default_args=default_args,
    description='DAG to import hourlies from Beacon Query API Output',
    schedule_interval='0 * * * *',
    catchup=False
)

beacon_conn_id='beacon_import_f5cs_conn'
destination_dir='/home/airflow/gcs/data'

t1 = F5BeaconMetricQueryHourlyExporterOperator(
    task_id='export_metrics_from_beacon',
    beacon_conn_id=beacon_conn_id,
    destination_dir=destination_dir,
    dag=dag
)

beacon_hook = BeaconHook(beacon_conn_id)
conn = beacon_hook.get_conn()
account_id = conn.extra_dejson['account_id']
dataset_name = "f5_beacon_%s" % account_id.lower().replace('-','_')
table_name_prefix = "beacon_metrics"

t2 = BashOperator(
    task_id='assure_dataset',
    bash_command='bq ls {} || bq mk {}'.format(dataset_name, dataset_name),
    dag=dag
)

import_command = """
cd {{ params.destination_dir }}/{{ run_id }}
for f in *.json; do
    bq load --noreplace --source_format=NEWLINE_DELIMITED_JSON {{ params.dataset_name }}.{{ params.table_name_prefix }}_{{ ds.replace('-', '_') }} $f ./schema
done
"""

t3 = BashOperator(
    task_id='import_data',
    bash_command=import_command,
    params={'destination_dir': destination_dir, 'dataset_name': dataset_name, 'table_name_prefix': table_name_prefix },
    dag=dag
)

cleanup_command="""
rm -rf {{ params.destination_dir }}/{{ run_id }}
"""

t4 = BashOperator(
    task_id='clean_up',
    trigger_rule=TriggerRule.ALL_DONE,
    bash_command=cleanup_command,
    params={'destination_dir': destination_dir},
    dag=dag
)

t1 >> t2 >> t3 >> t4


