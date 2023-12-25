# import requests
import datetime as dt
# import pytz

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable

from dags.utils.email import _send_successful_email_notification
from dags.utils.ca_utils import MySQL, BigQuery

with DAG(
    dag_id="period_ethereum_mainnet_us_transactions",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 21),
    schedule_interval="*/30 * * * *",
    params={
        "projectid": "int-data-ct-spotonchain",
        "partition_filter": dt.datetime.now().strftime("%Y-%m-%d"),
        "partition_field": "block_timestamp",
        "databaseschema":"spotonchain",
        "mysqltable": "transactions",
        "table_filter_date": "block_timestamp",
        "listResult": {"nextPageToken":""}
    },

) as dag:
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
        WHERE TIMESTAMP_TRUNC({partition_field}, DAY) = TIMESTAMP('{partition_filter}')
    """


    delete_query = """
        DELETE FROM {mysqltable} 
        WHERE {table_filter_date} = DATE(%s)
    """

    @task()
    def init_configuration(partition_filter, partition_field, mysqltable, table_filter_date):
        bq_query = query_base.format(
            partition_filter=partition_filter,
            partition_field=partition_field
        )

        mysql_delete = delete_query.format(
            mysqltable=mysqltable,
            table_filter_date=table_filter_date
        )

        return {"bq_query": bq_query, "mysql_delete": mysql_delete}


    @task()
    def delete_mysql(partition_filter, **kwargs):
        mysql_object = MySQL()
        mysql_delete = kwargs["init_config"]["mysql_delete"]

        mysql_object.execute(mysql_delete, (partition_filter, ))

        
   
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
                        partition_filter="{{ params.partition_filter }}", \
                        partition_field="{{ params.partition_field }}", \
                        mysqltable="{{ params.mysqltable }}", \
                        table_filter_date="{{ params.table_filter_date }}", \
                    )


    delete_mysql_table = delete_mysql(
                        partition_filter="{{ params.partition_filter }}", \
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
