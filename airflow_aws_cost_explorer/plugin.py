#!/usr/bin/env python
#
#   Copyright 2019 Andrea Bonomi <andrea.bonomi@gmail.com>
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the Licens
#
__author__ = 'Andrea Bonomi <andrea.bonomi@gmail.com>'
__version__ = '1.3.0'

from airflow.plugins_manager import AirflowPlugin
from airflow_aws_cost_explorer.operators.aws_cost_explorer import (
    AWSCostExplorerToS3Operator,
    AWSCostExplorerToLocalFileOperator,
)
from airflow_aws_cost_explorer.operators.aws_bucket_size import (
    AWSBucketSizeToLocalFileOperator,
    AWSBucketSizeToS3Operator
)

__all__ = [
    'AWSCostExplorerPlugin',
    'AWSCostExplorerToS3Operator',
    'AWSCostExplorerToLocalFileOperator',
    'AWSBucketSizeToLocalFileOperator',
    'AWSBucketSizeToS3Operator'
]

# Plugin
class AWSCostExplorerPlugin(AirflowPlugin):
    name = 'aws_cost_explorer'
    operators = [
        AWSCostExplorerToS3Operator,
        AWSCostExplorerToLocalFileOperator,
        AWSBucketSizeToLocalFileOperator,
        AWSBucketSizeToS3Operator
    ]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
    appbuilder_views = []

