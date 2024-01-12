import apache_beam as beam
from app.model.migration_model import MigrationProfileModel
from app.pipeline.demo_pipeline_task import LoadFromBigQueryToCloudSQL
import pandas as pd
from datetime import timedelta
from app.common.constant import (
    TIMEZONE, YYYY_MM_DD_HH_MM_SS_FF_FORMAT,
)
from app.common.utils.date_utils import get_current_local_datetime
from app.common.cloudlogging import log_task_failure, log_task_success





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


def execute_demo_pipeline(options, from_date, to_date, migrate_balance='false'):
    """
    function to execute pipeline
    @param pipeline: obj.
    @return: none.
    """
    migration_list = list()

    balance_query_string = """SELECT * FROM `bigquery-public-data.crypto_ethereum.balances`"""
    all_transfers_query_string = """
        SELECT * FROM `internal-blockchain-indexed.ethereum.all_transfers`
    """

    delete_query = """
        DELETE FROM soc_data_eth_db.ethereum_transfer_tab 
        WHERE txn_ts >= UNIX_TIMESTAMP(%s)
          AND txn_ts < UNIX_TIMESTAMP(%s)
    """

    balance_cloudsql_table_name = "balances"
    all_transfers_cloudsql_table_name = "ethereum_transfer_tab"

    start = get_current_local_datetime(
        timezone=TIMEZONE,
        datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
    )


    try:
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

    except Exception as e:
      log_task_failure(
          payload={
              "message": f"Failed to build migration profiles",
              "detail": {
                  "migration_profiles": beam_profiles,
                  "from_date": from_date,
                  "to_date": to_date,
                  "migrate_balance": migrate_balance,
                  "error_message": str(e)
              }
          }
      )

    log_task_success(
        payload={
            "message": f"Successful to build dataflow pipeline profiles",
            "detail": {
                "from_date": from_date,
                "to_date": to_date,
                "migrate_balance": migrate_balance,
            }
        },
        start_time=start
    )


    start = get_current_local_datetime(
        timezone=TIMEZONE,
        datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
    )

    log_task_success(
        payload={
            "message": f"Beginning the pipeline to ingest data from BigQuery to Cloud SQL with from_date {from_date} and to_date {to_date}",
            "detail": {
                "from_date": from_date,
                "to_date": to_date,
                "migrate_balance": migrate_balance,
            }
        },
        start_time=start
    )

    with beam.Pipeline(options=options) as pipeline:
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

    
    # log_task_success(
    #     payload={
    #         "message": f"Successful of the pipeline to ingest data from BigQuery to Cloud SQL with from_date {from_date} and to_date {to_date}",
    #         "detail": {
    #             "from_date": from_date,
    #             "to_date": to_date,
    #             "migrate_balance": migrate_balance,
    #         }
    #     },
    #     start_time=start
    # )