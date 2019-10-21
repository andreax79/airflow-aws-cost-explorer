#!/usr/bin/env python
#
#   Copyright 2019 Andrea Bonomi <andrea.bonomi@gmail.com>
#
#   Licensed under the Apache License, Version 2.0 (the 'License');
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an 'AS IS' BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the Licens
#

from enum import Enum
from datetime import timedelta
from airflow.utils.decorators import apply_defaults
from airflow_aws_cost_explorer.operators.commons import (
    AbstractOperator,
    DEFAULT_AWS_CONN_ID,
    DEFAULT_S3_CONN_ID,
    DEFAULT_FORMAT
)

__all__ = [
    'AWSCostExplorerToLocalFileOperator',
    'AWSCostExplorerToS3Operator',
    'Metric'
]

DEFAULT_METRICS = [ 'UnblendedCost', 'BlendedCost' ]
DTYPE = [('the_date', 'datetime64'), ('service', 'str'), ('metric', 'str'), ('amount', 'float64')]

Metric = Enum('Metric', 'AmortizedCost BlendedCost NetAmortizedCost NetUnblendedCost NormalizedUsageAmount UnblendedCost UsageQuantity')


class AbstractAWSCostExplorerOperator(AbstractOperator):

    dtype = DTYPE
    Metric = Metric

    def get_metrics_perform_query(self, ds, metrics, aws_hook, region_name):
        ce = aws_hook.get_client_type('ce', region_name=region_name)
        response = None
        next_token = None
        data = {
            'the_date': [],
            'service': [],
            'metric': [],
            'amount': []
        }
        while True:
            request = {
                'TimePeriod': {
                    'Start': ds.isoformat()[:10],
                    'End': (ds + timedelta(days=1)).isoformat()[:10]
                },
                'GroupBy': [
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ],
                'Metrics': metrics,
                'Granularity': 'DAILY'
            }
            if next_token is not None:
                request['NextPageToken'] = next_token
            response = ce.get_cost_and_usage(**request)
            next_token = response.get('NextPageToken', None)
            for item in response['ResultsByTime']:
                the_date = item['TimePeriod']['Start']
                for group in item['Groups']:
                    service = group['Keys'][0]
                    for metric in metrics:
                        amount = float(group['Metrics'][metric]['Amount'])
                        if amount > 0:
                            data['the_date'].append(the_date)
                            data['service'].append(service)
                            data['metric'].append(metric)
                            data['amount'].append(amount)
            if not next_token:
                break
        return data


class AWSCostExplorerToLocalFileOperator(AbstractAWSCostExplorerOperator):

    """
    AWS Cost Explorer to local file Operator

    :param day:             Date to be exported as string in YYYY-MM-DD format or date/datetime instance (default: yesterday)
    :type day:              str, date or datetime
    :param aws_conn_id:     Cost Explorer AWS connection id (default: aws_default)
    :type aws_conn_id:      str
    :param region_name:     Cost Explorer AWS Region
    :type region_name:      str
    :param destination:     Destination file complete path
    :type destination:      str
    :param file_format:     Destination file format (parquet, json or csv default: parquet)
    :type file_format:      str or FileFormat
    :param metrics:         Metrics (default: UnblendedCost, BlendedCost)
    :type metrics:          list
    """

    template_fields = [
        'destination',
        'day',
        'file_format',
        'aws_conn_id',
        'region_name',
    ]

    @apply_defaults
    def __init__(
        self,
        destination,
        day=None,
        file_format=DEFAULT_FORMAT,
        aws_conn_id=DEFAULT_AWS_CONN_ID,
        region_name=None,
        metrics=DEFAULT_METRICS,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.destination = destination
        self.day = day
        self.file_format = file_format
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.metrics = metrics.split(',') if isinstance(metrics, str) else metrics

    def execute(self, context):
        self.export_metrics(
            day=self.day,
            destination=self.destination,
            file_format=self.file_format,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            metrics=self.metrics
        )


class AWSCostExplorerToS3Operator(AbstractAWSCostExplorerOperator):

    """
    AWS Cost Explorer to S3 Operator

    :param day:             Date to be exported as string in YYYY-MM-DD format or date/datetime instance (default: yesterday)
    :type day:              str, date or datetime
    :param aws_conn_id:     Cost Explorer AWS connection id (default: aws_default)
    :type aws_conn_id:      str
    :param region_name:     Cost Explorer AWS Region
    :type region_name:      str
    :param s3_conn_id:      Destination S3 connection id (default: s3_default)
    :type s3_conn_id:       str
    :param s3_bucket:       Destination S3 bucket
    :type s3_bucket:        str
    :param s3_key:          Destination S3 key
    :type s3_key:           str
    :param file_format:     Destination file format (parquet, json or csv default: parquet)
    :type file_format:      str or FileFormat
    :param metrics:         Metrics (default: UnblendedCost, BlendedCost)
    :type metrics:          list
    """

    template_fields = [
        's3_bucket',
        's3_key',
        's3_conn_id',
        'day',
        'file_format',
        'aws_conn_id',
        'region_name',
    ]

    @apply_defaults
    def __init__(
        self,
        s3_bucket,
        s3_key,
        s3_conn_id=DEFAULT_S3_CONN_ID,
        day=None,
        file_format=DEFAULT_FORMAT,
        aws_conn_id=DEFAULT_AWS_CONN_ID,
        region_name=None,
        metrics=DEFAULT_METRICS,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.day = day
        self.file_format = file_format
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.metrics = metrics.split(',') if isinstance(metrics, str) else metrics

    def execute(self, context):
        self.export_metrics_to_s3(
            day=self.day,
            s3_conn_id=self.s3_conn_id,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            file_format=self.file_format,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            metrics=self.metrics
        )

