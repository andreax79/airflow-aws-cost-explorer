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

__all__ = [
    'AbstractOperator',
    'FileFormat',
    'DEFAULT_AWS_CONN_ID',
    'DEFAULT_S3_CONN_ID',
    'DEFAULT_FORMAT'
]

DEFAULT_AWS_CONN_ID = 'aws_default'
DEFAULT_S3_CONN_ID = 's3_default'
DEFAULT_FORMAT = 'parquet'

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


class AbstractOperator(BaseOperator):

    FileFormat = FileFormat

    def get_metrics(
            self,
            day,
            aws_conn_id=DEFAULT_AWS_CONN_ID,
            region_name=None,
            metrics=None):
        """
        Get the metrics

        :param day:             Date to be exported as string in YYYY-MM-DD format or date/datetime instance (default: yesterday)
        :type day:              str, date or datetime
        :param aws_conn_id:     AWS connection id (default: aws_default)
        :type aws_conn_id:      str
        :param region_name:     AWS Region
        :type region_name:      str
        :param metrics:         Metrics
        :type metrics:          list
        """
        aws_hook = AwsHook(aws_conn_id)
        region_name = region_name or aws_hook.get_session().region_name
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
        return self.get_metrics_perform_query(ds, metrics, aws_hook, region_name)

    def write_parquet(self, data, destination):
        # Write data to Parquet file
        try:
            import pandas as pd
            import numpy as np
        except:
            raise ValueError('Missing Parquet support - Please install pyarrow or fastparquet')
        if not data['the_date']: # If empty
            # Create an empty array with the correct data types
            data = np.empty(0, dtype=self.dtype)
        else:
            data['the_date'] = [dateutil.parser.parse(x) for x in data['the_date'] ]
        df = pd.DataFrame(data, columns=[ x[0] for x in self.dtype ])
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
        format_row = lambda row : dict([(x[0], row[i]) for i, x in enumerate(self.dtype)])
        data = [format_row(row) for row in zip(*[ data[x[0]] for x in self.dtype ])]
        with open(destination, 'w') as f:
            json.dump(data, f, indent=2)

    def write_csv(self, data, destination):
        # Write data to CSV file
        import csv
        data = zip(*[data[x[0]] for x in self.dtype])
        with open(destination, 'w') as f:
            csw_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            csw_writer.writerows(data)

    def export_metrics(self, day, destination, file_format, aws_conn_id, region_name, metrics):
        """
        Get the metrics and save the result to a file

        :param day:             Date to be exported as string in YYYY-MM-DD format or date/datetime instance (default: yesterday)
        :type day:              str, date or datetime
        :param destination:     Destination file complete path
        :type destination:      str
        :param file_format:     Destination file format (parquet, json or csv default: parquet)
        :type file_format:      str or FileFormat
        :param aws_conn_id:     AWS connection id (default: aws_default)
        :type aws_conn_id:      str
        :param region_name:     AWS Region
        :type region_name:      str
        :param metrics:         Metrics
        :type metrics:          list
        """
        # Query API
        data = self.get_metrics(day, aws_conn_id, region_name, metrics)
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

    def export_metrics_to_s3(self, day, s3_conn_id, s3_bucket, s3_key, file_format, aws_conn_id, region_name, metrics):
        """
        Get the metrics and save the result to S3

        :param day:             Date to be exported as string in YYYY-MM-DD format or date/datetime instance (default: yesterday)
        :type day:              str, date or datetime
        :param s3_conn_id:      Destination S3 connection id (default: s3_default)
        :type s3_conn_id:       str
        :param s3_bucket:       Destination S3 bucket
        :type s3_bucket:        str
        :param s3_key:          Destination S3 key
        :type s3_key:           str
        :param file_format:     Destination file format (parquet, json or csv default: parquet)
        :type file_format:      str or FileFormat
        :param aws_conn_id:     AWS connection id (default: aws_default)
        :type aws_conn_id:      str
        :param region_name:     AWS Region
        :type region_name:      str
        :param metrics:         Metrics
        :type metrics:          list
        """
        s3_hook = S3Hook(s3_conn_id)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                # Extract to a local file
                self.export_metrics(
                    day=day,
                    destination=tmp.name,
                    file_format=file_format,
                    aws_conn_id=aws_conn_id,
                    region_name=region_name,
                    metrics=metrics
                )
                # Upload the file to S3
                s3_hook.load_file(
                    filename=tmp.name,
                    key=s3_key,
                    bucket_name=s3_bucket,
                    replace=True
                )
            finally:
                # Delete the local file
                try:
                    os.delete(tmp.name)
                except:
                    pass

