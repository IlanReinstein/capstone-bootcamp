import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator

import sql_queries


def print_postgres_info():
    cloudsql = PostgresHook("cloudsql")
    print(cloudsql)
    print(cloudsql.run("SELECT 1"))
# def load_data_to_cloudsql(*args, **kwargs):
#     aws_hook = AwsHook("aws_credentials")
#     credentials = aws_hook.get_credentials()
#     redshift_hook = PostgresHook("cloudsql")
#     redshift_hook.run(sql_queries.COPY_ALL_USER_PURCHASE_SQL.format(credentials.access_key, credentials.secret_key))

# upload_gdrive_to_gcs = GoogleDriveToGCSOperator(
#     task_id="upload_gdrive_object_to_gcs",
#     folder_id='my-drive',
#     file_name='user-purchase.csv',
#     bucket_name='capstone-ir',
# )

dag = DAG(
    'postgres_test_conn',
    start_date=datetime.datetime.now()
)
#
# create_table = PostgresOperator(
#     task_id="create_table",
#     dag=dag,
#     postgres_conn_id="gcp",
#     sql=sql_statements.CREATE_USER_PURCHASE_TABLE
# )
#
print_instance = PythonOperator(
    task_id='testing_conn',
    python_callable=print_postgres_info,
    dag=dag
)
# copy_task = PythonOperator(
#     task_id='load_from_gcs_to_cloudsql',
#     dag=dag,
#     python_callable=load_data_to_cloudsql
# )
#
#
# create_table >> copy_task
