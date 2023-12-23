import pandas as pd
from ca_utils import MySQL, BigQuery




if __name__=="__main__":
    # mysql_object = MySQL()

    # print(mysql_object)

    bq_object=BigQuery()

    query_str = """
        SELECT
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
        WHERE TIMESTAMP_TRUNC(timestamp, DAY) >= TIMESTAMP('2023-12-21')
            AND TIMESTAMP_TRUNC(timestamp, DAY) <= TIMESTAMP('2023-12-22')
    """
    bq_connection = bq_object.get_connection("US")
    
    count = 0

    for chunk in pd.read_sql(sql=query_str, con=bq_connection, chunksize=200000):
        count += 1
        print(chunk)
    
    print(count)