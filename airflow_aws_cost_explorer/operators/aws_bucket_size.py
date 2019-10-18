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
    FileFormat,
    DEFAULT_AWS_CONN_ID,
    DEFAULT_S3_CONN_ID,
    DEFAULT_FORMAT
)

__all__ = [
    'AWSBucketSizeToLocalFileOperator',
    'AWSBucketSizeToS3Operator',
    'Metric',
    'FileFormat'
]

DEFAULT_METRICS = [ 'bucket_size', 'number_of_objects' ]
DTYPE = [('the_date', 'datetime64'), ('bucket_name', 'str'), ('bucket_size', 'uint'), ('number_of_objects', 'uint')]

Metric = Enum('Metric', 'bucket_size number_of_objects')


class AbstractAWSBucketSizeOperator(AbstractOperator):

    dtype = DTYPE

    def get_metric(self, ds, cloudwatch, bucket_name, metric_name, storage_type, statistic='Average'):
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/S3',
            MetricName=metric_name,
            Dimensions=[
                {'Name': 'BucketName',  'Value': bucket_name},
                {'Name': 'StorageType', 'Value': storage_type}
            ],
            Statistics=[statistic],
            Period=86400,
            StartTime=ds.isoformat(),
            EndTime=(ds + timedelta(days=1)).isoformat()
        )
        if response["Datapoints"]:
            return int(response['Datapoints'][0][statistic])
        else:
            return None

    def get_metrics_perform_query(self, ds, metrics, aws_hook, region_name):
        cloudwatch = aws_hook.get_client_type('cloudwatch', region_name=region_name)
        the_date = ds.isoformat()[:10]
        data = {
            'the_date': [],
            'bucket_name': [],
            'bucket_size': [],
            'number_of_objects': [],
        }
        # Get a list of all buckets
        s3 = aws_hook.get_client_type('s3', region_name=region_name)
        buckets = s3.list_buckets()
        # Iterate through each bucket
        for bucket in buckets['Buckets']:
            data['the_date'].append(the_date)
            data['bucket_name'].append(bucket['Name'])
            data['bucket_size'].append(self.get_metric(ds, cloudwatch, bucket['Name'], 'BucketSizeBytes', 'StandardStorage'))
            data['number_of_objects'].append(self.get_metric(ds, cloudwatch, bucket['Name'], 'NumberOfObjects', 'AllStorageTypes'))
        return data


class AWSBucketSizeToLocalFileOperator(AbstractAWSBucketSizeOperator):

    """
    AWS Bucket Size to local file Operator

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
    :param metrics:         Metrics (default: bucket_size, number_of_objects)
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


class AWSBucketSizeToS3Operator(AbstractAWSBucketSizeOperator):

    """
    AWS Bucket Size to S3 Operator

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
    :param metrics:         Metrics (default: bucket_size, number_of_objects)
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

