# import os
import time
# import requests
import datetime as dt
# import pytz

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable

from dags.utils.email import _send_successful_email_notification
from dags.utils.ca_utils import MySQL, BigQuery

query_base = """
SELECT 
  block_hash,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  nonce,
  from_address,
  to_address,
  value_lossless
  gas,
  gas_price,
  input,
  max_fee_per_gas,
  max_priority_fee_per_gas,
  transaction_type,
  chain_id
FROM `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions`
WHERE TIMESTAMP_TRUNC({partition_field}, DAY) >= TIMESTAMP('{from_date}')
    AND TIMESTAMP_TRUNC({partition_field}, DAY) <= TIMESTAMP('{to_date}')
"""


delete_query = """
    DELETE FROM {table} 
    WHERE {table_filter_date} >= DATE(%s)
        AND {table_filter_date} <= DATE(%s)
"""

with DAG(
    dag_id="init_ethereum_mainnet_us_transactions",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 17),
    schedule_interval=None,
    params={
        "projectid": "int-data-ct-spotonchain",
        "from_date": "2023-12-17",
        "to_date": "2023-12-18",
        "partition_field": "block_timestamp",
        "databaseschema":"spotonchain",
        "mysqltable": "transactions",
        "table_filter_date": "block_timestamp",
        "listResult": {"nextPageToken":""}
    },

) as dag:

    @task()
    def init_configuration(from_date, to_date, partition_field, mysqltable, table_filter_date):
        bq_query = query_base.format(
            from_date=from_date,
            to_date=to_date,
            partition_field=partition_field
        )

        mysql_delete = delete_query.format(
            table=mysqltable,
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
    def insert_data_to_sql(mysqltable, dataframe, **kwargs):
        mysql_object = MySQL()

        mysql_object.write(dataframe, mysqltable)

            
    init_config = init_configuration(
                            from_date="{{ params.from_date }}", \
                            to_date="{{ params.to_date }}", \
                            partition_field="{{ params.partition_field }}", \
                            mysqltable="{{ params.mysqltable }}", \
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
                        mysqltable="{{ params.mysqltable }}", \
                        dataframe=read_data
                    )
    
    init_config >> delete_mysql_table >> read_data >> write_data


globals()[dag.dag_id] = dag
