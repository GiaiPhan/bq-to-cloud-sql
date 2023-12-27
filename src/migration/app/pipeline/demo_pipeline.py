import apache_beam as beam
from app.model.migration_model import MigrationProfileModel
from app.pipeline.demo_pipeline_task import LoadFromBigQueryToCloudSQL
import pandas as pd
from datetime import timedelta



def convert_to_beam_profiles(migration_list):
    """
    function to convert beam to profile
    @param migration_list: List.
    @return: result: List.
    """
    result = []
    for info in migration_list:
        result.append(info.__dict__)

    return result


def execute_demo_pipeline(pipeline, from_date, to_date, migrate_balance='false'):
    """
    function to execute pipeline
    @param pipeline: obj.
    @return: none.
    """
    migration_list = list()

    balance_query_string = """SELECT * FROM `bigquery-public-data.crypto_ethereum.balances`"""
    all_transfers_query_string = """
        WITH transfers_token AS (
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
          WHERE TIMESTAMP_TRUNC(tt.block_timestamp, DAY) >= TIMESTAMP('{from_date}')
            AND TIMESTAMP_TRUNC(tt.block_timestamp, DAY) <= TIMESTAMP('{to_date}')
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
            AND TIMESTAMP_TRUNC(block_timestamp, DAY) >= TIMESTAMP('{from_date}')
            AND TIMESTAMP_TRUNC(block_timestamp, DAY) <= TIMESTAMP('{to_date}')
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
            AND TIMESTAMP_TRUNC(block_timestamp, DAY) >= TIMESTAMP('{from_date}')
            AND TIMESTAMP_TRUNC(block_timestamp, DAY) <= TIMESTAMP('{to_date}')
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
          transfers_withdrawal
    """

    delete_query = """
        DELETE FROM spotonchain_demo.all_transfers 
        WHERE txn_ts >= UNIX_TIMESTAMP(%s)
          AND txn_ts < UNIX_TIMESTAMP(%s)
    """

    balance_cloudsql_table_name = "balances"
    all_transfers_cloudsql_table_name = "all_transfers"


    start_date = from_date

    for _single_date in pd.date_range(from_date, to_date):
      all_transfers_migration_profile = MigrationProfileModel(
          query_string=all_transfers_query_string.format(
            from_date=start_date,
            to_date=_single_date.strftime("%Y-%m-%d")
          ),
          cloudsql_table_name=all_transfers_cloudsql_table_name,
          delete_query=delete_query,
          from_date=start_date,
          to_date=_single_date.strftime("%Y-%m-%d")
      )
      
      migration_list.append(all_transfers_migration_profile)

      start_date = (_single_date + timedelta(days=1)).strftime("%Y-%m-%d")

      if _single_date == to_date:
        break

    balance_migration_profile = MigrationProfileModel(
        query_string=balance_query_string,
        cloudsql_table_name=balance_cloudsql_table_name,
        delete_query=delete_query,
        from_date=from_date,
        to_date=to_date
    )
    if migrate_balance == 'true':
      migration_list.append(balance_migration_profile)

    beam_profiles = convert_to_beam_profiles(migration_list)

    profiles = (
        pipeline
        | 'Read migration config'
        >> beam.Create(beam_profiles)
    )

    _ = (
        profiles
        | 'Batching from BigQuery to Cloud SQL'
        >> beam.ParDo(LoadFromBigQueryToCloudSQL())
    )
