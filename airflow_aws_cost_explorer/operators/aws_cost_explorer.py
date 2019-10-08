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

import tempfile
import os
from datetime import datetime, date, timedelta
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

__all__ = [
    'AWSCostExplorerToLocalFileOperator',
    'AWSCostExplorerToS3Operator',
]

DEFAULT_AWS_CONN_ID = 'aws_default'
DEFAULT_S3_CONN_ID = 's3_default'
METRIC_UNBLENDED_COST = 'UnblendedCost'
FORMAT_PARQUET = 'parquet'
FORMAT_JSON = 'json'
FORMAT_CSV = 'csv'

class AbstractAWSCostExplorerOperator(BaseOperator):

    def query_cost_explorer(
            self,
            day,
            aws_conn_id=DEFAULT_AWS_CONN_ID,
            region_name=None,
            metric=METRIC_UNBLENDED_COST):
        """
        Query Cost Explorer API

        :param day:             Date to be exported as YYYY-MM-DD (default: yesterday)
        :type day:              str, date or datetime
        :param aws_conn_id:     Cost Explorer AWS connection id (default: aws_default)
        :type aws_conn_id:      str
        :param region_name:     Cost Explorer AWS Region
        :type region_name:      str
        :param metric:          Metric (default: UnblendedCost)
        :type metric:           str
        """

        aws_hook = AwsHook(aws_conn_id)
        region_name = region_name or aws_hook.get_session().region_name
        ce = aws_hook.get_client_type('ce', region_name=region_name)
        if day is None:
            ds = datetime.today() - timedelta(days=1)
        elif isinstance(day, date):
            ds = day
        else:
            ds = datetime.fromisoformat(day)
        self.log.info('day: %s aws_conn_id: %s region_name: %s metric: %s' % (
            day,
            aws_conn_id,
            region_name,
            metric))

        response = None
        next_token = None
        data = {
            'the_date': [],
            'service': [],
            'amount': []
        }
        while True:
            request = {
                'TimePeriod': {
                    'Start': (ds - timedelta(days=1)).isoformat()[:10],
                    'End': ds.isoformat()[:10]
                },
                'GroupBy': [
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ],
                'Metrics': [ metric ],
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
                    amount = float(group['Metrics'][metric]['Amount'])
                    if amount > 0:
                        data['the_date'].append(the_date)
                        data['service'].append(service)
                        data['amount'].append(amount)
            if not next_token:
                break
        return data

    def write_parquet(self, data, destination):
        # Write data to Parquet file
        import pandas as pd
        import numpy as np
        import fastparquet
        data['the_date'] = np.array(data['the_date'], dtype=np.datetime64)
        df = pd.DataFrame(data, columns=data.keys())
        fastparquet.write(destination, df)

    def write_json(self, data, destination):
        # Write data to JSON file
        import json
        data = [{ 'the_date': x[0], 'service': x[1], 'amount': x[2] }
                for x in zip(data['the_date'], data['service'], data['amount'])]
        with open(destination, 'w') as f:
            json.dump(data, f, indent=2)

    def write_csv(self, data, destination):
        # Write data to CSV file
        import csv
        with open(destination, 'w') as f:
            csw_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            csw_writer.writerows(zip(data['the_date'], data['service'], data['amount']))

    def export_cost_explorer(self, day, destination, file_format, aws_conn_id, region_name, metric):
        """
        Query AWS Cost Explorer and save result to file

        :param day:             Date to be exported as YYYY-MM-DD (default: yesterday)
        :type day:              str, date or datetime
        :param destination:     Destination file complete path
        :type destination:      str
        :param file_format:     Destination file format (parquet, json or csv default: parquet)
        :type file_format:      str
        :param aws_conn_id:     Cost Explorer AWS connection id (default: aws_default)
        :type aws_conn_id:      str
        :param region_name:     Cost Explorer AWS Region
        :type region_name:      str
        :param metric:          Metric (default: UnblendedCost)
        :type metric:           str
        """
        # Query Cost Explorer API
        data = self.query_cost_explorer(day, aws_conn_id, region_name, metric)
        # Write data to file
        file_format = file_format.lower() if file_format else FORMAT_PARQUET
        if file_format == FORMAT_PARQUET:
            self.write_parquet(data, destination)
        elif file_format == FORMAT_JSON:
            self.write_json(data, destination)
        elif file_format == FORMAT_CSV:
            self.write_csv(data, destination)
        else:
            raise ValueError('Invalid file format: {}'.format(file_format))


class AWSCostExplorerToLocalFileOperator(AbstractAWSCostExplorerOperator):

    """
    AWS Cost Explorer to local file Operator

    :param day:             Date to be exported as YYYY-MM-DD (default: yesterday)
    :type day:              str, date or datetime
    :param aws_conn_id:     Cost Explorer AWS connection id (default: aws_default)
    :type aws_conn_id:      str
    :param region_name:     Cost Explorer AWS Region
    :type region_name:      str
    :param destination:     Destination file complete path
    :type destination:      str
    :param file_format:     Destination file format (parquet, json or csv default: parquet)
    :type file_format:      str
    :param metric:          Metric (default: UnblendedCost)
    :type metric:           str
    """

    template_fields = [
        'destination',
        'day',
        'file_format',
        'aws_conn_id',
        'region_name',
        'metric'
    ]

    @apply_defaults
    def __init__(
        self,
        destination,
        day=None,
        file_format=FORMAT_PARQUET,
        aws_conn_id=DEFAULT_AWS_CONN_ID,
        region_name=None,
        metric=METRIC_UNBLENDED_COST,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.destination = destination
        self.day = day
        self.file_format = file_format
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.metric = metric

    def execute(self, context):
        self.export_cost_explorer(
            day=self.day,
            destination=self.destination,
            file_format=self.file_format,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            metric=self.metric
        )


class AWSCostExplorerToS3Operator(AbstractAWSCostExplorerOperator):

    """
    AWS Cost Explorer to S3 Operator

    :param day:             Date to be exported as YYYY-MM-DD (default: yesterday)
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
    :type file_format:      str
    :param metric:          Metric (default: UnblendedCost)
    :type metric:           str
    """

    template_fields = [
        's3_bucket',
        's3_key',
        's3_conn_id',
        'day',
        'file_format',
        'aws_conn_id',
        'region_name',
        'metric'
    ]

    @apply_defaults
    def __init__(
        self,
        s3_bucket,
        s3_key,
        s3_conn_id=DEFAULT_S3_CONN_ID,
        day=None,
        file_format=FORMAT_PARQUET,
        aws_conn_id=DEFAULT_AWS_CONN_ID,
        region_name=None,
        metric=METRIC_UNBLENDED_COST,
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
        self.metric = metric

    def execute(self, context):
        s3_hook = S3Hook(self.s3_conn_id)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                # Extract to a local file
                self.export_cost_explorer(
                    day=self.day,
                    destination=tmp.name,
                    file_format=self.file_format,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    metric=self.metric
                )
                # Upload the file to S3
                s3_hook.load_file(
                    filename=tmp.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True
                )
            finally:
                # Delete the local file
                try:
                    os.delete(tmp.name)
                except:
                    pass

