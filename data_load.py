import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator

import sql_queries


def load_data_to_cloudsql(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_queries.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))

upload_gdrive_to_gcs = GoogleDriveToGCSOperator(
    task_id="upload_gdrive_object_to_gcs",
    folder_id=FOLDER_ID,
    file_name=FILE_NAME,
    bucket_name=BUCKET,
    object_name=OBJECT,
)

dag = DAG(
    'load_users_purchases',
    start_date=datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="gcp",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

copy_task = PythonOperator(
    task_id='load_from_gcs_to_cloudsql',
    dag=dag,
    python_callable=load_data_to_redshift
)


create_table >> copy_task
copy_task >> location_traffic_task
