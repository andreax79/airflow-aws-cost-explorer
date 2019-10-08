#!/usr/bin/env python
import airflow
from airflow import DAG
from airflow_aws_cost_explorer import AWSCostExplorerToS3Operator, AWSCostExplorerToLocalFileOperator
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
    # destination='/tmp/{{ yesterday_ds }}.json',
    # file_format='json',
    # destination='/tmp/{{ yesterday_ds }}.csv',
    # file_format='csv',
    dag=dag)

# aws_cost_explorer_to_s3 = AWSCostExplorerToS3Operator(
#     task_id='aws_cost_explorer_to_s3',
#     day='{{ ds }}',
#     s3_conn='aws_default',
#     s3_bucket='{{ var.value.datalake_bucket }}',
#     s3_key='cost_explorer/{{ execution_date.strftime("%Y") }}/month={{ execution_date.strftime("%m") }}/day={{ execution_date.strftime("%d") }}/cost_explorer.parquet',
#     dag=dag)

if __name__ == "__main__":
    dag.cli()
