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
This module contains F5 Beacon metric API exporter
"""

import time
import datetime
import json
import os
import re
import subprocess

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from beacon_export_plugin.hooks.beacon_hook import BeaconHook

from requests.exceptions import HTTPError
from line_protocol_parser import parse_line

# schema test7
SCHEMA_SKEL = [
    {'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'account_id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'source_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'evt', 'type': 'RECORD', 'mode': 'NULLABLE',
     'fields': [
         {'name': 'version', 'type': 'STRING', 'mode': 'NULLABLE'},
         {'name': 'sourceName', 'type': 'STRING', 'mode': 'NULLABLE'},
         {'name': 'sourceDescription', 'type': 'STRING', 'mode': 'NULLABLE'},
         {'name': 'fields', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': []},
         {'name': 'tags', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': []},
         {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
     ]
     }
]

WRITE_FILE_DELAY_SECS = 1


class F5BeaconMetricQueryExporterOperator(BaseOperator):

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 beacon_conn_id: str = 'f5_beacon_default',
                 destination_dir: str = '/home/airflow/gcs/data',
                 start_timestamp: int = 0,
                 stop_timestamp: int = 0,
                 metric_file: str = None,
                 schema_file: str = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.beacon_conn_id = beacon_conn_id
        self.destination_dir = destination_dir
        self.start_timestamp = start_timestamp
        self.stop_timestamp = stop_timestamp
        self.beacon_hook = BeaconHook(beacon_conn_id)

        self.schema = SCHEMA_SKEL

        self.mfn = metric_file
        self.sfn = schema_file

        self.tags = {}
        self.fields = {}

    def execute(self, context):
        if not self.mfn:
            self.mfn = self.get_metrics_fn(context['dag_run'].run_id)
        if not self.sfn:
            self.sfn = self.get_schema_fn(context['dag_run'].run_id)
        conn = self.beacon_hook.get_conn()
        account_id = 'primary'
        if 'account_id' in conn.extra_dejson:
            account_id = conn.extra_dejson['account_id']
        self.log.info('Executing extract metrics from f5 Beacon account %s between %s:%s into: %s',
                      account_id, self.start_timestamp, self.stop_timestamp, self.destination_dir)
        known_measurements = self.beacon_hook.get_measurements()
        self.log.info('found %s measurement for f5 Beacon account %s',
                      known_measurements, account_id)
        if os.path.exists(self.mfn):
            os.unlink(self.mfn)
        for measurement in known_measurements:
            self.get_measurement_records(
                measurement, context['dag_run'].run_id)
        self.write_schema(context['dag_run'].run_id)

    def get_metrics_fn(self, run_id):
        dest_dir = os.path.join(self.destination_dir, run_id)
        os.makedirs(dest_dir, exist_ok=True)
        return os.path.join(dest_dir, 'line_metrics.json')

    def get_schema_fn(self, run_id):
        dest_dir = os.path.join(self.destination_dir, run_id)
        os.makedirs(dest_dir, exist_ok=True)
        return os.path.join(dest_dir, 'line_schema.json')

    def get_field(self, field_name):
        field_name = self.format_col_name(field_name)
        key = field_name.lower()
        if key in self.fields:
            return self.fields[key]
        return None

    def get_tag(self, tag_name):
        tag_name = self.format_col_name(tag_name)
        key = tag_name.lower()
        if key in self.tags:
            return self.tags[key]
        return None

    def get_measurement_records(self, account_id, measurement, run_id):
        batch_size = 9000
        have_records = True
        start_timestamp = int(self.start_timestamp)
        stop_timestamp = int(self.stop_timestamp) + 1
        offset_seconds = 0
        self.load_schema()
        while have_records:
            if offset_seconds > 0:
                start_timestamp = offset_seconds
            query = "SELECT * FROM \"%s\" WHERE time > %s000000000 and time < %s000000000 ORDER BY time LIMIT %s" % (
                measurement, start_timestamp, stop_timestamp, batch_size)
            self.log.info(
                'submitting query: %s to f5 Beacon metric API.', query)
            try:
                line_data = self.beacon_hook.query_metric(
                    query, output_line=True)
                records = line_data.split("\n")
                number_of_records = len(records)
                if number_of_records:
                    self.log.info('writing %d records from f5 Beacon metrics API to %s',
                                  number_of_records, self.destination_dir)
                    offset_seconds = self.output_to_file(
                        records, run_id, account_id)
                if number_of_records < batch_size:
                    have_records = False
            except HTTPError as he:
                self.log.error(he)
                if batch_size > 1:
                    batch_size = int(batch_size*0.9)
                else:
                    raise AirflowException(
                        'could not export f5 Beacon metric for measurement %s after reducing record limit to 1', measurement)
        self.save_schema()

    def load_schema(self):
        if os.path.exists(self.sfn):
            with open(self.sfn, 'r') as sf:
                try:
                    self.schema = json.load(sf)
                    if not self.schema:
                        self.schema = SCHEMA_SKEL
                except json.JSONDecodeError:
                    self.schema = SCHEMA_SKEL
        self.populate_cols_from_schema()

    def populate_cols_from_schema(self):
        # reduce tags and fields through dict
        for col in self.schema:
            if col['name'] == 'evt':
                for event_cols in col['fields']:
                    if event_cols['name'] == 'fields':
                        for field_cols in event_cols['fields']:
                            key = field_cols['name'].lower()
                            self.fields[key] = field_cols['name']
                    if event_cols['name'] == 'tags':
                        for tag_cols in event_cols['fields']:
                            key = tag_cols['name'].lower()
                            self.tags[key] = tag_cols['name']

    def save_schema(self):
        with open(self.sfn, 'w+') as sf:
            json.dump(self.schema, sf, indent=4,
                      separators=(',', ': '))
            time.sleep(WRITE_FILE_DELAY_SECS)

    def format_col_name(self, existing_tag):
        # converting to Camel case or all lower case
        components = existing_tag.split('_')
        converted = components[0] + ''.join(x.title() for x in components[1:])
        components = converted.split('-')
        converted = components[0] + ''.join(x.title() for x in components[1:])
        if converted.isupper():
            converted = converted.lower()
        if converted[0].isalpha() and converted[0].isupper():
            converted = converted[0].lower() + converted[1:]
        if not (converted[0].isalpha() or converted[0] == '_'):
            converted = "beacon%s" % converted
        return converted

    def add_field_to_schema(self, field_name, field_type):
        field_name = self.format_col_name(field_name)
        for col in self.schema:
            if col['name'] == 'evt':
                for c in col['fields']:
                    if c['name'] == 'fields':
                        c['fields'].append(
                            {
                                'name': field_name,
                                'type': field_type,
                                'mode': 'NULLABLE'
                            }
                        )
        key = field_name.lower()
        self.fields[key] = field_name
        return field_name

    def add_tag_to_schema(self, tag_name):
        tag_name = self.format_col_name(tag_name)
        for col in self.schema:
            if col['name'] == 'evt':
                for c in col['fields']:
                    if c['name'] == 'tags':
                        c['fields'].append(
                            {
                                'name': tag_name,
                                'type': 'STRING',
                                'mode': 'NULLABLE'
                            }
                        )
        key = tag_name.lower()
        self.tags[key] = tag_name
        return tag_name

    def output_to_file(self, lines, run_id, account_id):
        df = open(self.mfn, 'a+')
        largest_timestamp = 0
        for line in lines:
            if line:
                data = parse_line(line)
                # Transform
                ms_timestamp = float(int(data['time']) / 1000000000)
                fields_dict = {}
                for fn in data['fields']:
                    val = data['fields'][fn]
                    tfn = self.get_field(fn)
                    if not tfn:
                        if type(val) == bool:
                            tfn = self.add_field_to_schema(fn, 'BOOL')
                        elif type(val) == int:
                            tfn = self.add_field_to_schema(fn, 'INT64')
                        elif type(val) == float:
                            tfn = self.add_field_to_schema(fn, 'FLOAT64')
                        else:
                            tfn = self.add_field_to_schema(fn, 'STRING')
                    fields_dict[tfn] = val
                tags_dict = {}
                for tn in data['tags']:
                    ttn = self.get_tag(tn)
                    if not ttn:
                        ttn = self.add_tag_to_schema(tn)
                    tags_dict[ttn] = str(data['tags'][tn])
                transformed_data = {
                    'ts': ms_timestamp,
                    'account_id': "urn:f5_cs::acccount:%s" % account_id,
                    'source_id': None,
                    'evt': {
                        'version': '1.0',
                        'sourceName': data['measurement'],
                        'sourceDescription': "data imported from beacon for account %s" % account_id,
                        'fields': fields_dict,
                        'tags': tags_dict,
                        'timestamp': ms_timestamp
                    }
                }
                df.write("%s\n" % json.dumps(transformed_data))
                if ms_timestamp > largest_timestamp:
                    largest_timestamp = int(ms_timestamp)
        time.sleep(WRITE_FILE_DELAY_SECS)
        df.close()
        return largest_timestamp


class F5BeaconMetricQueryDailyExporterOperator(F5BeaconMetricQueryExporterOperator):

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 beacon_conn_id: str = 'f5_beacon_default',
                 destination_dir="/home/airflow/gcs/data",
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.beacon_conn_id = beacon_conn_id
        self.destination_dir = destination_dir
        self.beacon_hook = BeaconHook(self.beacon_conn_id)
        self.date = None

    def execute(self, context):
        if not self.mfn:
            self.mfn = self.get_metrics_fn(context['dag_run'].run_id)
        if not self.sfn:
            self.sfn = self.get_schema_fn(context['dag_run'].run_id)
        self.date = str(context.get("execution_date").date())
        conn = self.beacon_hook.get_conn()
        account_id = 'primary'
        if 'account_id' in conn.extra_dejson:
            account_id = conn.extra_dejson['account_id']
        self.log.info('Executing extract metrics from f5 Beacon account %s on %s into: %s',
                      account_id, self.date, self.destination_dir)
        known_measurements = self.beacon_hook.get_measurements()
        self.log.info('found %s measurement for f5 Beacon account %s',
                      known_measurements, account_id)
        self.start_timestamp = start_timestamp = int(time.mktime(
            datetime.datetime.strptime(self.date, '%Y-%m-%d').timetuple()))
        self.stop_timestamp = start_timestamp + 86400
        if os.path.exists(self.mfn):
            os.unlink(self.mfn)
        for measurement in known_measurements:
            self.get_measurement_records(
                account_id, measurement, context['dag_run'].run_id)


class F5BeaconMetricQueryHourlyExporterOperator(F5BeaconMetricQueryExporterOperator):

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 beacon_conn_id: str = 'f5_beacon_default',
                 destination_dir="/home/airflow/gcs/data",
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.beacon_conn_id = beacon_conn_id
        self.destination_dir = destination_dir
        self.beacon_hook = BeaconHook(self.beacon_conn_id)

    def execute(self, context):
        if not self.mfn:
            self.mfn = self.get_metrics_fn(context['dag_run'].run_id)
        if not self.sfn:
            self.sfn = self.get_schema_fn(context['dag_run'].run_id)
        conn = self.beacon_hook.get_conn()
        account_id = 'primary'
        if 'account_id' in conn.extra_dejson:
            account_id = conn.extra_dejson['account_id']
        self.stop_timestamp = int(time.mktime(
            context.get("execution_date").timetuple()))
        self.start_timestamp = self.stop_timestamp - 3600
        self.log.info('Executing extract metrics from f5 Beacon account %s for %s - %s into: %s',
                      account_id, self.start_timestamp, self.stop_timestamp, self.destination_dir)
        known_measurements = self.beacon_hook.get_measurements()
        self.log.info('found %s measurement for f5 Beacon account %s',
                      known_measurements, account_id)
        if os.path.exists(self.mfn):
            os.unlink(self.mfn)
        for measurement in known_measurements:
            self.get_measurement_records(
                account_id, measurement, context['dag_run'].run_id)
