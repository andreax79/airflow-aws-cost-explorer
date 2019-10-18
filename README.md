# Airflow AWS Cost Explorer Plugin
A plugin for [Apache Airflow](https://github.com/apache/airflow) that allows
you to export [AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/)
as [S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/cloudwatch-monitoring.html) metrics to
local file or S3 in Parquet, JSON, or CSV format.

### System Requirements

* Airflow Versions
    * 1.10.3 or newer
* pyarrow or fastparquet (optional, for writing Parquet files)

### Deployment Instructions

1. Install the plugin

    pip install airflow-aws-cost-explorer

2. Optional for writing Parquet files - Install pyarrow or fastparquet

    pip install pyarrow

    or

    pip install fastparquet

2. Restart the Airflow Web Server

3. Configure the AWS connection (Conn type = 'aws')

4. Optional for S3 - Configure the S3 connection (Conn type = 's3')

## Operators

### AWSCostExplorerToS3Operator
```AWS Cost Explorer to S3 Operator

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

```

### AWSCostExplorerToLocalFileOperator
```AWS Cost Explorer to local file Operator

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

```

### AWSBucketSizeToS3Operator
```AWS Bucket Size to S3 Operator

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

```

### AWSBucketSizeToLocalFileOperator
```AWS Bucket Size to local file Operator

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

```

### Example

```
    #!/usr/bin/env python
    import airflow
    from airflow import DAG
    from airflow_aws_cost_explorer import AWSCostExplorerToLocalFileOperator
    from datetime import timedelta

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(1),
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=30)
    }

    dag = DAG('cost_explorer',
        default_args=default_args,
        schedule_interval=None,
        concurrency=1,
        max_active_runs=1,
        catchup=False
    )

    aws_cost_explorer_to_file = AWSCostExplorerToLocalFileOperator(
        task_id='aws_cost_explorer_to_file',
        day='{{ yesterday_ds }}',
        destination='/tmp/{{ yesterday_ds }}.parquet',
        file_format='parquet',
        dag=dag)

    if __name__ == "__main__":
        dag.cli()
```

### Links

* Apache Airflow - https://github.com/apache/airflow
* Apache Arrow - https://github.com/apache/arrow
* fastparquet - https://github.com/dask/fastparquet
* AWS Cost Explorer - https://aws.amazon.com/aws-cost-management/aws-cost-explorer/ [API Reference](https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetCostAndUsage.html)
* S3 CloudWatch Metrics - https://docs.aws.amazon.com/AmazonS3/latest/dev/cloudwatch-monitoring.html
