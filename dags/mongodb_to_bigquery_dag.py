from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'mongodb_to_bigquery_pipeline',
    start_date=datetime(2024, 10, 23),
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Run PySpark ETL Script to GCS
spark_etl_task = SparkSubmitOperator(
    task_id='mongodb_to_gcs',
    application='/path/to/mongodb_to_gcs.py',
    conn_id='spark_default',
    dag=dag
)

# Task 2: Run PySpark Script to Parse Billing Period
parse_task = SparkSubmitOperator(
    task_id='parse_billing_period',
    application='/path/to/parse_billing_period.py',
    conn_id='spark_default',
    dag=dag
)

# Task 3: Load Invoices from GCS to BigQuery
load_invoices_to_bq = GCSToBigQueryOperator(
    task_id='load_invoices_to_bq',
    bucket='<bucket-name>',
    source_objects=['invoices/*.parquet'],
    destination_project_dataset_table='project.dataset.invoices',
    source_format='PARQUET',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

# Task 4: Load Subscriptions from GCS to BigQuery
load_subscriptions_to_bq = GCSToBigQueryOperator(
    task_id='load_subscriptions_to_bq',
    bucket='<bucket-name>',
    source_objects=['subscriptions/*.parquet'],
    destination_project_dataset_table='project.dataset.subscriptions',
    source_format='PARQUET',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

# Task 5: Load Parsed Payments from GCS to BigQuery
load_payments_to_bq = GCSToBigQueryOperator(
    task_id='load_payments_to_bq',
    bucket='<bucket-name>',
    source_objects=['parsed_payments/*.parquet'],
    destination_project_dataset_table='project.dataset.subscription_payments',
    source_format='PARQUET',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

# Set Task Dependencies
spark_etl_task >> [parse_task, load_invoices_to_bq, load_subscriptions_to_bq] >> load_payments_to_bq
