# import os
import time
# import requests
import datetime as dt
# import pytz

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable

from utils.email import _send_successful_email_notification
from dags.utils.ca_utils import MySQL, BigQuery

query_base = """
    SELECT * FROM `bigquery-public-data.crypto_ethereum.balances`
"""

with DAG(
    dag_id="period_crypto_ethereum_balances",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 21),
    schedule_interval=None,
    params={
        "table_type": "one-time",
        "projectid": "int-data-ct-spotonchain",
        "databaseschema":"spotonchain",
        "table": "balances",
        "primary_key": "address",
        "other_columns": ["eth_balance",],
        "listResult": {"nextPageToken":""}
    },

) as dag:
   
    @task()
    def query_bq_table(projectid, **kwargs):
        bq_object = BigQuery(project=projectid)

        result = bq_object.query_to_dataframe(query_base)

        return result


    @task()
    def insert_data_to_sql(table, dataframe, primary_key, other_columns, **kwargs):
        mysql_object = MySQL()

        mysql_object.update_by_column(
            df=dataframe, 
            table_name=table, 
            primary_key=primary_key,
            other_columns=other_columns
        )


    read_data = query_bq_table(
                    projectid="{{ params.projectid }}"
                )
    
    write_data = insert_data_to_sql(
                        table="{{ params.table }}",
                        dataframe=read_data,
                        primary_key="{{ params.primary_key }}",
                        other_columns="{{ params.other_columns }}",
                    )
    
    read_data >> write_data


globals()[dag.dag_id] = dag
