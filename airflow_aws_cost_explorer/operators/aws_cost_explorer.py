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
import dateutil.parser
from enum import Enum
from datetime import datetime, date, timedelta
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

__all__ = [
    'AWSCostExplorerToLocalFileOperator',
    'AWSCostExplorerToS3Operator',
    'Metric',
    'FileFormat'
]

DEFAULT_AWS_CONN_ID = 'aws_default'
DEFAULT_S3_CONN_ID = 's3_default'
DEFAULT_FORMAT = 'parquet'
DEFAULT_METRICS = [ 'UnblendedCost', 'BlendedCost' ]

Metric = Enum('Metric', 'AmortizedCost BlendedCost NetAmortizedCost NetUnblendedCost NormalizedUsageAmount UnblendedCost UsageQuantity')


class FileFormat(Enum):
    parquet = 'parquet'
    json = 'json'
    csv = 'csv'

    @classmethod
    def lookup(cls, file_format):
        if not file_format:
            return DEFAULT_FORMAT
        elif isinstance(file_format, FileFormat):
            return file_format
        else:
            return cls[file_format.lower()]


class AbstractAWSCostExplorerOperator(BaseOperator):

    def query_cost_explorer(
            self,
            day,
            aws_conn_id=DEFAULT_AWS_CONN_ID,
            region_name=None,
            metrics=DEFAULT_METRICS):
        """
        Query Cost Explorer API

        :param day:             Date to be exported as string in YYYY-MM-DD format or date/datetime instance (default: yesterday)
        :type day:              str, date or datetime
        :param aws_conn_id:     Cost Explorer AWS connection id (default: aws_default)
        :type aws_conn_id:      str
        :param region_name:     Cost Explorer AWS Region
        :type region_name:      str
        :param metrics:         Metrics (default: UnblendedCost, BlendedCost)
        :type metrics:          list
        """

        aws_hook = AwsHook(aws_conn_id)
        region_name = region_name or aws_hook.get_session().region_name
        ce = aws_hook.get_client_type('ce', region_name=region_name)
        if not day or (isinstance(day, str) and day.lower() == 'yesterday'):
            ds = datetime.today() - timedelta(days=1)
        elif isinstance(day, date): # datetime is a subclass of date
            ds = day
        else:
            ds = dateutil.parser.parse(day)
        self.metrics = metrics.split(',') if isinstance(metrics, str) else metrics
        self.log.info('ds: {ds:%Y-%m-%d} aws_conn_id: {aws_conn_id} region_name: {region_name} metrics: {metrics}'.format(
            ds=ds,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            metrics=metrics))

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

    def write_parquet(self, data, destination):
        # Write data to Parquet file
        try:
            import pandas as pd
            import numpy as np
        except:
            raise ValueError('Missing Parquet support - Please install pyarrow or fastparquet')
        if not data['the_date']: # If empty
            # Create an empty array with the correct data types
            data = np.empty(0, dtype=[('the_date', 'datetime64'), ('service', 'str'), ('metric', 'str'), ('amount', 'float64')])
        else:
            data['the_date'] = [ dateutil.parser.parse(x) for x in data['the_date'] ]
        df = pd.DataFrame(data, columns=[ 'the_date', 'service', 'metric', 'amount' ])
        try:
            import pyarrow.parquet
            pyarrow.parquet.write_table(
                    table=pyarrow.Table.from_pandas(df),
                    where=destination,
                    compression='SNAPPY',
                    flavor='spark')
        except ImportError:
            try:
                import fastparquet
                fastparquet.write(destination, df, times='int96')
            except:
                raise ValueError('Missing Parquet support - Please install pyarrow or fastparquet')

    def write_json(self, data, destination):
        # Write data to JSON file
        import json
        data = [{ 'the_date': x[0], 'service': x[1], 'metric': x[2], 'amount': x[3] }
                for x in zip(data['the_date'], data['service'], data['metric'], data['amount'])]
        with open(destination, 'w') as f:
            json.dump(data, f, indent=2)

    def write_csv(self, data, destination):
        # Write data to CSV file
        import csv
        with open(destination, 'w') as f:
            csw_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            csw_writer.writerows(zip(data['the_date'], data['service'], data['metric'], data['amount']))

    def export_cost_explorer(self, day, destination, file_format, aws_conn_id, region_name, metrics):
        """
        Query AWS Cost Explorer and save result to file

        :param day:             Date to be exported as string in YYYY-MM-DD format or date/datetime instance (default: yesterday)
        :type day:              str, date or datetime
        :param destination:     Destination file complete path
        :type destination:      str
        :param file_format:     Destination file format (parquet, json or csv default: parquet)
        :type file_format:      str or FileFormat
        :param aws_conn_id:     Cost Explorer AWS connection id (default: aws_default)
        :type aws_conn_id:      str
        :param region_name:     Cost Explorer AWS Region
        :type region_name:      str
        :param metrics:         Metrics (default: UnblendedCost, BlendedCost)
        :type metrics:          list
        """
        # Query Cost Explorer API
        data = self.query_cost_explorer(day, aws_conn_id, region_name, metrics)
        # Write data to file
        file_format = FileFormat.lookup(file_format)
        if file_format == FileFormat.parquet:
            self.write_parquet(data, destination)
        elif file_format == FileFormat.json:
            self.write_json(data, destination)
        elif file_format == FileFormat.csv:
            self.write_csv(data, destination)
        else:
            raise ValueError('Invalid file format: {}'.format(file_format))


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
        self.export_cost_explorer(
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
                    metrics=self.metrics
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

