import datetime
import sqlalchemy
from sqlalchemy import create_engine

from pandas.io import sql
from google.cloud import bigquery



class MySQL:

    def __init__(self, 
                 host="35.240.228.251", user="user_test", 
                 password="/6g+u$`#f=nOgMk'", database="spotonchain"
    ):
        self.db = create_engine('mysql://{user}:{password}@{host}/{database}'.format(
            host=host, user=user, 
            password=password, database=database
        )).connect()

    

    def execute(self, query, params):
        self.db.execute(query, params)

    def write(self, df, table_name, if_exists='append'):
        df.to_sql(
            con=self.db, 
            name=table_name, 
            index=False,
            if_exists=if_exists, chunksize=20000
        )


class BigQuery:

    def __init__(self, project="int-data-ct-spotonchain"):
        self.client = bigquery.Client(project=project)

    def query_to_dataframe(self, query):
        df = self.client.query(query).to_dataframe()

        return df



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



test_mysql = MySQL()
test_bq = BigQuery()


test_mysql.execute(
    """ DELETE FROM eth 
    WHERE txn_ts >= DATE(%s)
        AND txn_ts <= DATE(%s)""", ('2023-12-17', '2023-12-28',))

df = test_bq.query_to_dataframe(
    query_base.format(
        from_date='2023-12-17',
        to_date='2023-12-18',
    )
)

# df["txn_ts"] = df['txn_ts'].apply(lambda x: x.strftime('%Y-%m-%d'))

test_mysql.write(df, table_name='eth')