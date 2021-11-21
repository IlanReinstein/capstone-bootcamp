import datetime
import logging
import pandas as pd
import psycopg2
import io

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

import sql_queries

# FOLDER_ID = '1nj2AXJG10DTjSjL0J17WlDdZeunJQ9r6'#'1rgBurNjLUqErTcaLx0TB53FNWEsausiJhP9Oa3goJSzlv3_xaAjBD8tPz4ROmSIq96KSHggH'
# FILE_NAME = 'user_purchase.csv'#



def load_csv(*args, **kwargs):
    table = 'user_purchase'
    cloudsql_hook = PostgresHook(postgres_conn_id="cloudsql")
    conn = cloudsql_hook.get_conn()
    cursor = conn.cursor()
    fpath = '~/user_purchase.csv'

    with open(fpath, 'r') as f:
        next(f)
        cursor.copy_expert("COPY users.user_purchase FROM STDIN WITH CSV HEADER", f)
        conn.commit()
    #
    # # CSV loading to table
    # with open(file_path("cities_clean.csv"), "r") as f:
    #     next(f)
    #     curr.copy_from(f, 'cities', sep=",")
    #     get_postgres_conn.commit()
    # print("loading csv done")
    # cursor.close()

dag = DAG(
    'load_users_table',
    start_date=datetime.datetime.now()
)

download_file = GCSToLocalFilesystemOperator(
        dag=dag,
        task_id="download_file",
        object_name='gs://ir-raw-data/user_purchase.csv',
        bucket='ir-raw-data',
        filename='~/user_purchase.csv'
    )

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

download_file >> create_table >> copy_from_gcs
