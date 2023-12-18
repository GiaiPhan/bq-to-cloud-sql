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
    WHERE TIMESTAMP_TRUNC(trans.block_timestamp, DAY) >= TIMESTAMP('{from_date}')
        AND TIMESTAMP_TRUNC(trans.block_timestamp, DAY) <= TIMESTAMP('{to_date}')
"""


delete_query = """
    DELETE FROM {table} 
    WHERE {table_filter_date} >= DATE(%s)
        AND {table_filter_date} <= DATE(%s)
"""

with DAG(
    dag_id="insert_bq_data_to_sql",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 17),
    schedule_interval=None,
    params={
        "projectid": "int-data-ct-spotonchain",
        "from_date": "2023-12-17",
        "to_date": "2023-12-18",
        "instance":"spotonchain",
        "databaseschema":"spotonchain",
        "table": "eth",
        "table_filter_date": "txn_ts",
        "listResult": {"nextPageToken":""}
    },

) as dag:

    @task()
    def init_configuration(projectid, from_date, to_date, table, table_filter_date):
        bq_query = query_base.format(
            from_date=from_date,
            to_date=to_date
        )

        mysql_delete = delete_query.format(
            table=table,
            table_filter_date=table_filter_date
        )

        return {"bq_query": bq_query, "mysql_delete": mysql_delete}

    @task()
    def delete_mysql(from_date, to_date, **kwargs):
        mysql_object = MySQL()
        mysql_delete = kwargs["init_config"]["mysql_delete"]

        mysql_object.execute(mysql_delete, (from_date, to_date, ))

        
   
    @task()
    def query_bq_table(projectid, **kwargs):
        bq_object = BigQuery(project=projectid)
        query_str = kwargs["init_config"]["bq_query"]

        result = bq_object.query_to_dataframe(query_str)

        return result



    @task()
    def insert_data_to_sql(table, dataframe, **kwargs):
        mysql_object = MySQL()

        mysql_object.write(dataframe, table)

            
    init_config = init_configuration(
                            projectid="{{ params.projectid }}", \
                            from_date="{{ params.from_date }}", \
                            to_date="{{ params.to_date }}", \
                            table="{{ params.table }}", \
                            table_filter_date="{{ params.table_filter_date }}", \
                        )


    delete_mysql_table = delete_mysql(
                        from_date="{{ params.from_date }}", \
                        to_date="{{ params.to_date }}", \
                        init_config=init_config
                    )

    read_data = query_bq_table(
                        projectid="{{ params.projectid }}", \
                        init_config=init_config
                    )
    
    write_data = insert_data_to_sql(
                        table="{{ params.table }}", \
                        dataframe=read_data
                    )
    
    init_config >> delete_mysql_table >> read_data >> write_data


globals()[dag.dag_id] = dag
