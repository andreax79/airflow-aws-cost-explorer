#!/usr/bin/env python

import os
import os.path
import sys
import dateutil.parser
from datetime import datetime, timedelta
from airflow.operators.aws_cost_explorer import AWSCostExplorerToLocalFileOperator

def main():
    if len(sys.argv) < 2:
        print('usage: {} <START_DATE> [END_DATE]'.format(sys.argv[0]))
        sys.exit(2)

    t0 = dateutil.parser.parse(sys.argv[1])
    if len(sys.argv) == 2:
        t1 = datetime.now() - timedelta(days=1)
    else:
        t1 = dateutil.parser.parse(sys.argv[2])
    t = t0
    while t <= t1:
        print('{year:04d}-{month:02d}-{day:02d}'.format(year=t.year, month=t.month, day=t.day))
        path = 'cost_explorer/year={year:04d}/month={month:02d}/day={day:02d}'.format(year=t.year, month=t.month, day=t.day)
        os.makedirs(path, exist_ok=True)
        op = AWSCostExplorerToLocalFileOperator(
            task_id='export',
            destination=os.path.join(path, 'cost_explorer.parquet'),
            file_format='parquet',
            day=t)
        op.execute(None)
        t = t + timedelta(days=1)

if __name__ == '__main__':
    main()
