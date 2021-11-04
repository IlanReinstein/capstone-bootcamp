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
    # table = pd.read_csv('gs://capstone-ir/user_purchase.csv')
    # records = table.to_records(index=False).tolist()
    redshift_hook = PostgresHook("cloudsql")
    buf = io.BytesIO()
    redshift_hook.copy_expert(sql_queries.COPY_ALL_USER_PURCHASE_SQL, buf)

# def copy_from_stringio(conn, df, table):
#     """
#     Here we are going save the dataframe in memory
#     and use copy_from() to copy it to the table
#     """
#     # save dataframe to an in memory buffer
#     buffer = StringIO()
#     df.to_csv(buffer, index_label='id', header=False)
#     buffer.seek(0)
#
#     cursor = conn.cursor()
#     try:
#         cursor.copy_from(buffer, table, sep=",")
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#         conn.rollback()
#         cursor.close()
#         return 1
#     print("copy_from_stringio() done")
#     cursor.close()

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
    task_id='load_csv_to_gcs',
    dag=dag,
    python_callable=load_csv,
)
#
# copy_task = PostgresOperator(
#     task_id='load_from_gcs_to_cloudsql',
#     dag=dag,
#     postgres_conn_id="cloudsql",
#     sql=sql_queries.COPY_ALL_USER_PURCHASE_SQL
# )

# detect_file >> upload_gdrive_to_gcs >>
create_table >> copy_from_gcs
