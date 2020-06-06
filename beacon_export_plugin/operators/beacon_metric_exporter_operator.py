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
import logging
import os
import subprocess

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from beacon_export_plugin.hooks.beacon_hook import BeaconHook

from requests.exceptions import HTTPError
from line_protocol_parser import parse_line


class F5BeaconMetricQueryExporterOperator(BaseOperator):

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 beacon_conn_id: str = 'f5_beacon_default',
                 destination_dir: str = '/home/airflow/gcs/data',
                 start_timestamp: int = 0,
                 stop_timestamp: int = 0,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.beacon_conn_id = beacon_conn_id
        self.destination_dir = destination_dir
        self.start_timestamp = start_timestamp
        self.stop_timestamp = stop_timestamp
        self.beacon_hook = BeaconHook(self.beacon_conn_id)

    def execute(self, context):
        connection = self.beacon_hook.get_conn()
        self.log.info('Executing extract metrics from f5 Beacon account %s between %s:%s into: %s',
                      self.beacon_hook.extras['account_id'], self.start_timestamp, self.stop_timestamp, self.destination_dir)
        known_measurements = self.beacon_hook.get_measurements()
        self.log.info('found %s measurement for f5 Beacon account %s',
                      known_measurements, self.beacon_hook.extras['account_id'])
        fn = self.get_metrics_fn(context['dag_run'].run_id)
        if os.path.exists(fn):
            os.unlink(fn)
        for measurement in known_measurements:
            self.get_measurement_records(
                measurement, context['dag_run'].run_id)
        self.write_schema(context['dag_run'].run_id)

    def get_measurement_records(self, measurement, run_id):
        batch_size = 9000
        have_records = True
        start_timestamp = int(self.start_timestamp)
        stop_timestamp = int(self.stop_timestamp)
        offset_seconds = 0
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
                    offset_seconds = self.output_to_file(records, run_id)
                if number_of_records < batch_size:
                    have_records = False
            except HTTPError as he:
                self.log.error(he)
                if batch_size > 1:
                    batch_size = int(batch_size*0.9)
                else:
                    raise AirflowException(
                        'could not export f5 Beacon metric for measurement %s after reducing record limit to 1', measurement)

    def output_to_file(self, lines, run_id):
        fn = self.get_metrics_fn(run_id)
        of = open(fn, 'a+')
        largest_timestamp = 0
        for line in lines:
            if line:
                data = parse_line(line)
                data['time'] = float(
                    '.'.join([str(data['time'])[:-9], str(data['time'])[-9:]]))
                if data['time'] > largest_timestamp:
                    largest_timestamp = int(data['time'])
                dt = datetime.datetime.fromtimestamp(int(data['time']))
                data['datetime'] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                tags_list = []
                for tn in data['tags']:
                    tags_list.append({
                        'tag_name': tn,
                        'tag_value': str(data['tags'][tn])
                    })
                data['tags'] = tags_list
                fields_list = []
                for fn in data['fields']:
                    fields_list.append({
                        'field_name': fn,
                        'field_value': str(data['fields'][fn])
                    })
                data['fields'] = fields_list
                of.write("%s\n" % json.dumps(data))
        time.sleep(1)
        of.close()
        return largest_timestamp

    def get_metrics_fn(self, run_id):
        dest_dir = os.path.join(self.destination_dir, run_id)
        os.makedirs(dest_dir, exist_ok=True)
        return os.path.join(dest_dir, 'line_metrics.json')

    def write_schema(self, run_id):
        schema = [{'name': 'measurement', 'type': 'STRING', 'mode': 'REQUIRED'},
                  {'name': 'tags',
                   'type': 'RECORD',
                   'mode': 'REPEATED',
                   'fields': [{'name': 'tag_name', 'type': 'STRING', 'mode': 'REQUIRED'},
                              {'name': 'tag_value', 'type': 'STRING', 'mode': 'NULLABLE'}]},
                  {'name': 'fields',
                   'type': 'RECORD',
                   'mode': 'REPEATED',
                   'fields': [{'name': 'field_name', 'type': 'STRING', 'mode': 'REQUIRED'},
                              {'name': 'field_value', 'type': 'STRING', 'mode': 'NULLABLE'}]},
                  {'name': 'time', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                  {'name': 'datetime', 'type': 'DATETIME', 'mode': 'REQUIRED'}]
        dest_dir = os.path.join(self.destination_dir, run_id)
        fn = "%s/schema" % dest_dir
        with open(fn, 'w+') as sf:
            sf.write(json.dumps(schema))


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
        self.date = str(context.get("execution_date").date())
        connection = self.beacon_hook.get_conn()
        self.log.info('Executing extract metrics from f5 Beacon account %s on %s into: %s',
                      self.beacon_hook.extras['account_id'], self.date, self.destination_dir)
        known_measurements = self.beacon_hook.get_measurements()
        self.log.info('found %s measurement for f5 Beacon account %s',
                      known_measurements, self.beacon_hook.extras['account_id'])
        self.start_timestamp = start_timestamp = int(time.mktime(
            datetime.datetime.strptime(self.date, '%Y-%m-%d').timetuple()))
        self.stop_timestamp = start_timestamp + 86400
        fn = self.get_metrics_fn(context['dag_run'].run_id)
        if os.path.exists(fn):
            os.unlink(fn)
        for measurement in known_measurements:
            self.get_measurement_records(
                measurement, context['dag_run'].run_id)
        self.write_schema(context['dag_run'].run_id)
