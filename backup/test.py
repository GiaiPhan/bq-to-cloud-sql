import pandas as pd
import time
# import requests
import datetime as dt
# import pytz

from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.models import Variable

from utils.email import _send_successful_email_notification
from dags.utils.ca_utils import BigQuery

query_base = """
        SELECT 
        trans.`hash` as txn_hash, 
        trans.block_hash as block_hash, 
        trans.transaction_type as transfer_type, 
        trans.transaction_index as ref_index, 
        trans.block_number as block_number, 
        trans.from_address as from_address, 
        trans.to_address as to_address, 
        cont.address as contract_address, 
        trans.value as quantity, 
        trans.block_timestamp as txn_ts, 
        tok.symbol as token_id, 
        trans.block_hash as operator_address, 
        EXTRACT(HOUR from trans.block_timestamp) as update_time, 
    FROM `bigquery-public-data.crypto_ethereum.transactions`  trans 
    INNER JOIN `bigquery-public-data.crypto_ethereum.contracts` cont 
        ON trans.block_number = cont.block_number 
    INNER JOIN `bigquery-public-data.crypto_ethereum.tokens` tok 
        ON trans.block_number = tok.block_number 
    WHERE TIMESTAMP_TRUNC(trans.block_timestamp, DAY) = TIMESTAMP('{query_date}')
"""

with DAG(
    dag_id="test",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 17),
    schedule_interval=None,
    params={
        "projectid": "int-data-ct-spotonchain",
        "from_date": "2023-12-17",
        "to_date": "2023-12-18",
        "listResult": {"nextPageToken":""}
    },

) as dag:

    @task()
    def start_task():
        print("Hello")


    @task()
    def end_stask():
        print("Goodbye")


    @task()
    def init_configuration(query_date):
        bq_query = query_base.format(
            query_date=query_date
        )

        return {"bq_query": bq_query}

    @task()
    def query_bq_table(projectid, **kwargs):
        bq_object = BigQuery(project=projectid)
        query_str = kwargs["init_config"]["bq_query"]

        result = bq_object.query_to_dataframe(query_str)

        return result


    @task_group()
    def test_group(projectid, query_date, **kwargs):
        query_bq_table(projectid, init_configuration(query_date=query_date))


    date_list = pd.date_range(start="{{ params.from_date }}",end="{{ params.to_date }}")

    start = start_task()
    end = end_stask()

    for _date in date_list:
        loop_task = test_group(
                projectid="{{ params.projectid }}", \
                query_date = _date
            )

        start >> loop_task >> end

    
globals()[dag.dag_id] = dag
