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
This module contains F5 Beacon Plugin definition
"""

from airflow.plugins_manager import AirflowPlugin
from beacon_export_plugin.hooks.beacon_hook import BeaconHook
from beacon_export_plugin.operators.beacon_metric_exporter_operator import F5BeaconMetricQueryExporterOperator

class AirflowF5BeaconPlugin(AirflowPlugin):
    name = "f5_beacon_plugin"
    operators = [F5BeaconMetricQueryExporterOperator]
    sensors = []
    hooks = [BeaconHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
