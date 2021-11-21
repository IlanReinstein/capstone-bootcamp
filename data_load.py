import datetime
import logging
import pandas as pd
import io

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator

import sql_queries

# FOLDER_ID = '1nj2AXJG10DTjSjL0J17WlDdZeunJQ9r6'#'1rgBurNjLUqErTcaLx0TB53FNWEsausiJhP9Oa3goJSzlv3_xaAjBD8tPz4ROmSIq96KSHggH'
# FILE_NAME = 'user_purchase.csv'#


def load_csv(*args, **kwargs):
    table = 'user_purchase'
    cloudsql_hook = PostgresHook("cloudsql")
    # buffer = StringIO()
    # df.to_csv(buffer, header=False)
    # buffer.seek(0)
    fpath = 'https://storage.cloud.google.com/ir-raw-data/user_purchase.csv'
    conn = cloudsql_hook.get_conn()
    cursor = conn.cursor()
    try:
        with open(fpath, 'r') as f:
            next(f)
            cursor.copy_from(buffer, table, sep=",")
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("loading csv done")
    cursor.close()

dag = DAG(
    'load_users_table',
    start_date=datetime.datetime.now()
)

# upload_gdrive_to_gcs = GoogleDriveToGCSOperator(
#     task_id="upload_gdrive_to_gcs",
#     folder_id=FOLDER_ID,
#     file_name=FILE_NAME,
#     bucket_name='capstone-ir',
# )
#
# detect_file = GoogleDriveFileExistenceSensor(
#     task_id="detect_file",
#     folder_id=FOLDER_ID,
#     file_name=FILE_NAME,
#     dag = dag
# )

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="cloudsql",
    sql=sql_queries.CREATE_USER_PURCHASE_TABLE
)


copy_from_gcs = PythonOperator(
    task_id='copy_csv_to_cloudsql',
    dag=dag,
    python_callable=load_csv,
)

create_table >> copy_from_gcs
