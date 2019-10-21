#!/usr/bin/env python

import os
import os.path
import argparse
import dateutil.parser
from datetime import date, timedelta
from airflow.operators.aws_cost_explorer import AWSCostExplorerToLocalFileOperator, AWSBucketSizeToLocalFileOperator

def main():
    FileFormat = AWSCostExplorerToLocalFileOperator.FileFormat
    StorageType = AWSBucketSizeToLocalFileOperator.StorageType

    parser = argparse.ArgumentParser()
    parser.add_argument('type', choices=['cost_explorer', 'bucket_size'])
    parser.add_argument('--start_date', default=(date.today() - timedelta(days=1)).isoformat())
    parser.add_argument('--end_date', default=(date.today() - timedelta(days=1)).isoformat())
    parser.add_argument('--format', choices=[x.value for x in FileFormat], default='parquet')
    parser.add_argument('--storage_types', choices=[x.value for x in StorageType], default=None, nargs='*')
    args = parser.parse_args()

    t0 = dateutil.parser.parse(args.start_date)
    t1 = dateutil.parser.parse(args.end_date)
    t = t0
    if args.type == 'cost_explorer':
        op = AWSCostExplorerToLocalFileOperator
    else:
        op = AWSBucketSizeToLocalFileOperator
    while t <= t1:
        print('{year:04d}-{month:02d}-{day:02d}'.format(year=t.year, month=t.month, day=t.day))
        path = '{type}/year={year:04d}/month={month:02d}/day={day:02d}'.format(type=args.type, year=t.year, month=t.month, day=t.day)
        os.makedirs(path, exist_ok=True)
        op_args = dict(
            task_id='export',
            destination=os.path.join(path, '{type}.{format}'.format(type=args.type, format=args.format)),
            file_format=args.format,
            day=t
        )
        if args.storage_types:
            op_args['storage_types'] = args.storage_types
        op(**op_args).execute(None)
        t = t + timedelta(days=1)

if __name__ == '__main__':
    main()
