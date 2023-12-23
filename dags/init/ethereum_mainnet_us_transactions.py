# import os
import time
# import requests
import datetime as dt
# import pytz

from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.models import Variable

# from dags.utils.email import _send_successful_email_notification
from dags.utils.ca_utils import MySQL, BigQuery

import pandas as pd



query_base = """
`    WITH
    transfers_token AS (
    SELECT
        transaction_hash AS txn_hash,
        tt.block_hash,
        CASE
        WHEN ct.is_erc721 THEN 200   --ERC721 Token transfer
        ELSE 10                      --ERC20 Token transfer
        END AS transfer_type, 
        log_index AS ref_index,
        tt.block_number,
        UNIX_SECONDS(tt.block_timestamp) AS txn_ts,
        token_address AS contract_address,
        from_address,
        to_address,
        CASE
        WHEN ct.is_erc721 THEN value
        ELSE "0"
        END AS token_id,
        CASE 
        WHEN ct.is_erc721 THEN 1
        ELSE SAFE_CAST(value as BIGNUMERIC)/POW(10, 6)
        END AS quantity,
        "0x0000000000000000000000000000000000000000" AS operator_address,
        GENERATE_UUID() AS id
    FROM
        `bigquery-public-data.crypto_ethereum.token_transfers` AS tt
    LEFT JOIN 
        `bigquery-public-data.crypto_ethereum.contracts` AS ct
    ON 
        tt.token_address=ct.address
    WHERE TIMESTAMP_TRUNC(tt.{partition_field}, DAY) >= TIMESTAMP('{from_date}')
        AND TIMESTAMP_TRUNC(tt.{partition_field}, DAY) <= TIMESTAMP('{to_date}')
    ),
    transfers_external AS (
    SELECT
        `hash` AS txn_hash,
        block_hash,
        1 AS transfer_type, --Native transfer
        0 AS ref_index,
        block_number,
        UNIX_SECONDS(block_timestamp) AS txn_ts,
        "0x0000000000000000000000000000000000000000" AS contract_address,
        from_address,
        to_address,
        "0" AS token_id,
        CAST(value as BIGNUMERIC)/POW(10, 6) AS quantity,
        "0x0000000000000000000000000000000000000000" AS operator_address,
        GENERATE_UUID() AS id
    FROM
        `bigquery-public-data.crypto_ethereum.transactions`
    WHERE 
        value > 0
        AND TIMESTAMP_TRUNC({partition_field}, DAY) >= TIMESTAMP('{from_date}')
        AND TIMESTAMP_TRUNC({partition_field}, DAY) <= TIMESTAMP('{to_date}')
    ),

    transfers_internal AS (
    SELECT
        transaction_hash as txn_hash,
        block_hash,
        2 AS transfer_type,
        0 as ref_index,
        block_number,
        UNIX_SECONDS(block_timestamp) as txn_ts,
        "0x0000000000000000000000000000000000000000" as contract_address,
        from_address,
        to_address,
        "0" as token_id,
        CAST(value as BIGNUMERIC)/POW(10, 6) as quantity,
        "0x0000000000000000000000000000000000000000" AS operator_address,
        GENERATE_UUID() AS id
    FROM
        `bigquery-public-data.crypto_ethereum.traces` 
    WHERE 
        call_type != "delegatecall"
        AND from_address IS NOT NULL
        AND to_address IS NOT NULL
        AND value > 0
        AND TIMESTAMP_TRUNC({partition_field}, DAY) >= TIMESTAMP('{from_date}')
        AND TIMESTAMP_TRUNC({partition_field}, DAY) <= TIMESTAMP('{to_date}')
    ),

    transfers_withdrawal as (SELECT
        CAST(w.index AS STRING) as txn_hash,
        `hash`,
        3 AS transfer_type, -- Native withdrawal
        w.index as ref_index,
        number,
        UNIX_SECONDS(timestamp) as txn_ts,
        "0x0000000000000000000000000000000000000000" as contract_address,
        "0x0000000000000000000000000000000000000000" as from_address,
        w.address,
        "0" as token_id,
        CAST(w.amount AS BIGNUMERIC)/POW(10, 6) as quantity,
        "0x0000000000000000000000000000000000000000" AS operator_address,
        GENERATE_UUID() AS id
    FROM
        `bigquery-public-data.crypto_ethereum.blocks` AS b
        CROSS JOIN UNNEST(withdrawals) AS w
    WHERE TIMESTAMP_TRUNC(timestamp, DAY) >= TIMESTAMP('{from_date}')
        AND TIMESTAMP_TRUNC(timestamp, DAY) <= TIMESTAMP('{to_date}')
    )

    SELECT
    *
    FROM
    transfers_token
    UNION ALL
    SELECT
    *
    FROM
    transfers_external
    UNION ALL
    SELECT
    *
    FROM
    transfers_internal
    UNION ALL
    SELECT
    *
    FROM
    transfers_withdrawal`
"""


delete_query = """
    DELETE FROM {table} 
    WHERE {table_filter_date} >= CAST(UNIX_TIMESTAMP(%s) AS UNSIGNED)
        AND {table_filter_date} <= CAST(UNIX_TIMESTAMP(%s) AS UNSIGNED)
"""

with DAG(
    dag_id="init_ethereum_mainnet_us_transactions",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 17),
    schedule_interval=None,
    params={
        "project_id": "int-data-ct-spotonchain",
        "bq_location": "US",
        "from_date": "2023-12-21",
        "to_date": "2023-12-22",
        "partition_field": "block_timestamp",
        "chunksize": 100000,
        "databaseschema":"spotonchain_demo",
        "mysqltable": "all_transfers",
        "table_filter_date": "txn_ts",
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

        
    def insert_into_mysql(dataframe, mysqltable):
        mysql_object = MySQL()

        mysql_object.write(dataframe, mysqltable)


    @task()
    def insert_into_mysql_by_chunks(project_id, bq_location, mysqltable, chunksize, **kwargs):
        bq_object = BigQuery(project=project_id)
        query_str = kwargs["init_config"]["bq_query"]

        bq_connection = bq_object.get_connection(location=bq_location, chunksize=chunksize)

        for chunk in pd.read_sql(sql=query_str, con=bq_connection, chunksize=chunksize):
            print(chunk.columns)
            if not chunk.empty:
                insert_into_mysql(
                    dataframe=chunk,
                    mysqltable=mysqltable
                )


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
    
    insert_chunk_to_mysql = insert_into_mysql_by_chunks(
                                project_id="{{ params.project_id }}", \
                                bq_location="{{ params.bq_location }}", \
                                mysqltable="{{ params.mysqltable }}", \
                                chunksize="{{ params.chunksize }}", \
                                init_config=init_config
                            )


    # init_config >> delete_mysql_table  >> insert_chunk_to_mysql

    init_config >> insert_chunk_to_mysql

globals()[dag.dag_id] = dag
