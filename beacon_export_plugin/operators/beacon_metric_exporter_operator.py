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
                 date: str = None,
                 destination_dir="./data",
                 output_line: bool = True,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.beacon_conn_id = beacon_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.date = date
        self.destination_dir = destination_dir
        self.output_line = output_line

        self.output_loggers = {}

    def execute(self, context):
        hook = BeaconHook(self.beacon_conn_id)
        connection = hook.get_conn()
        self.log.info('Executing extract metrics from f5 Beacon account %s on %s into: %s',
                      hook.extras['account_id'], self.date, self.destination_dir)
        known_measurements = hook.get_measurements()
        self.log.info('found %s measurement for f5 Beacon account %s',
                      known_measurements, hook.extras['account_id'])
        for measurement in known_measurements:
            get_measurement_records(measurement, hook, self.date)

    def get_measurement_records(self, measurement, hook, date):
        batch_size = 5000
        current_offset = 0
        have_records = True
        start_timestamp = time.mktime(
            datetime.datetime.strptime(self.date, '%Y-%m-%d').timetuple())
        stop_timestamp = start_timestamp + 86400
        while have_records:
            query = "SELECT * FROM \"%s\" WHERE time > %s and time < %s LIMIT %s" % (
                measurement, start_timestamp, stop_timestamp, batch_size, current_offset)
            try:
                line_data = hook.query_metric(
                    query, output_line=self.output_line)
                records = line_data.split("\n")
                for record in records:
                    self.output_to_file(record, hook)
                if len(records) < batch_size:
                    have_records = False
                current_offset = current_offset + batch_size
            except HTTPError as he:
                self.log.error(he)
                if batch_size > 1:
                    batch_size = int(batch_size*0.9)
                else:
                    raise AirflowException(
                        'could not export f5 Beacon metric for measurement %s after reducing record limit to 1', measurement)

    def out_to_file(self, line):
        if line:
            data = parse_line(line)
            data['time'] = float(
                '.'.join([str(data['time'])[:-9], str(data['time'])[-9:]]))
            tags_list = []
            for tn in data['tags']:
                tags_list.append({
                    'name': tn,
                    'value': str(data['tags'][tn])
                })
            data['tags'] = tags_list
            fields_list = []
            for fn in data['fields']:
                fields_list.append({
                    'name': fn,
                    'value': str(data['fields'][fn])
                })
            data['fields'] = fields_list
            fn = get_metric_file(data['time'])
            if os.path.exists(fn):
                os.unlink(fn)
            if fn not in self.output_loggers:
                output = logging.getLogger(fn)
                output.setLevel(logging.INFO)
                lfh = logging.FileHandler(fn)
                lfh.setFormatter(logging.Formatter('%(message)s'))
                output.addHandler(lfh)
                output_loggers[fn] = output
            output_loggers[fn].info(json.dumps(data))

    def get_metric_file(self, timestamp, hook):
        ymd = str(datetime.date.fromtimestamp(timestamp))
        fn = os.path.join(self.destination_dir, "%s-%s-%s-%s" %
                          ('f5-beacon-metric-query-export-', hook.extras['account_id'], ymd, '.json'))
        return fn
