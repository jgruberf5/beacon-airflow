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
This module contains F5 Beacon service hook
"""
import json
import requests


from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from requests.exceptions import HTTPError


class BeaconHook(BaseHook):
    """
    Hook to interact with F5 Beacon.
    """

    def __init__(self, conn_id, *args, **kwargs):
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        self.connection = None
        self.token = None
        self.account_id = None

    def get_conn(self):
        if not self.connection:
            self.connection = self.get_connection(self.conn_id)
        return self.connection

    def assure_token(self):
        if not self.token:
            self.get_service_token()
        if not self.account_id:
            if not 'account_id' in self.connection.extra_dejson:
                self.get_account_info()
            else:
                self.account_id = self.connection.extra_dejson['account_id']

    def update_token(self):
        self.token = None
        self.get_service_token()

    def get_service_token(self):
        if self.connection.login and self.connection.password:
            try:
                headers = {
                    "Content-Type": "application/json"
                }
                data = {
                    "username": self.connection.login,
                    "password": self.connection.password
                }
                self.extras = self.connection.extra_dejson
                url = "https://%s/%s/svc-auth/login" % (
                    self.connection.host, self.connection.schema)
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                if response.status_code < 300:
                    self.token = response.json()['access_token']
                    return response.json()['access_token']
                else:
                    raise AirflowException('error retrieving f5 Beacon token: %d: %s' % (
                        response.status_code, response.content))
            except Exception as ex:
                raise AirflowException(
                    'exception retrieving f5 Beacon token: %s' % ex)
        else:
            raise AirflowException(
                'f5 beacon connection %s, does not have a login and password' % self.conn_id)

    def get_account_info(self):
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % self.token
            }
            url = "https://%s/%s/svc-account/user" % (
                self.connection.host, self.connection.schema)
            response = requests.get(url, headers=headers)
            if response.status_code < 300:
                data = response.json()
                self.account_id = data['primary_account_id']
            else:
                http_ex = HTTPError(
                    'error retrieving f5 Beacon account: %d: %s' % (response.status_code, response.content))
                http_ex.status_code = response.status_code
                raise http_ex
        except Exception as ex:
            raise AirflowException(
                'exception retrieveing f5 Beacon account: %s' % ex)

    def get_measurements(self):
        try:
            self.assure_token()
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % self.token,
                "X-F5aas-Preferred-Account-Id": self.account_id
            }
            url = "https://%s/beacon/%s/metrics" % (
                self.connection.host, self.connection.schema)
            data = {
                "query": "SHOW MEASUREMENTS"
            }
            response = requests.post(
                url, headers=headers, data=json.dumps(data))
            if response.status_code < 300:
                return_names = []
                resobj = response.json()['Results'][0]
                if resobj['Series']:
                    values = resobj['Series'][0]['values']
                    for value in values:
                        return_names.append(value[0])
                return return_names
            elif response.status_code == 401:
                self.update_token()
                headers['Authorization'] = "Bearer %s" % self.token
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                if response.status_code < 300:
                    return_names = []
                    resobj = response.json()['Results'][0]
                    if resobj['Series']:
                        values = resobj['Series'][0]['values']
                        for value in values:
                            return_names.append(value[0])
                    return return_names
                else:
                    http_ex = HTTPError(
                        'error retrieving f5 Beacon measurements: %d: %s' % (response.status_code, response.content))
                    http_ex.status_code = response.status_code
                    raise http_ex
            else:
                http_ex = HTTPError(
                    'error retrieving f5 Beacon measurements: %d: %s' % (response.status_code, response.content))
                http_ex.status_code = response.status_code
                raise http_ex
        except Exception as ex:
            raise AirflowException(
                'exception retrieveing f5 Beacon measurements: %s' % ex)

    def query_metric(self, query, output_line):
        try:
            self.assure_token()
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % self.token,
                "X-F5aas-Preferred-Account-Id": self.account_id
            }
            url = "https://%s/beacon/%s/metrics" % (
                self.connection.host, self.connection.schema)
            data = {
                "query": query
            }
            if output_line:
                data["outputformat"] = "INFLUXLINE"
            response = requests.post(
                url, headers=headers, data=json.dumps(data))
            if response.status_code < 300:
                return response.content.decode()
            elif response.status_code == 401:
                self.update_token()
                headers['Authorization'] = "Bearer %s" % self.token
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                if response.status_code < 300:
                    return response.content.decode()
                else:
                    raise AirflowException(
                        'error retrieving f5 Beacon measurements: %d: %s' % (response.status_code, response.content))
            else:
                raise AirflowException(
                    'error retrieving f5 Beacon measurements: %d: %s' % (response.status_code, response.content))
        except Exception as ex:
            raise AirflowException(
                'exception retrieveing measurements: %s' % ex)
