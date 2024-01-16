import apache_beam as beam
from app.model.migration_model import MigrationProfileModel
from app.pipeline.demo_pipeline_task import LoadFromBigQueryToCloudSQL
from app.config.application_config import PROJECT_ID

import pandas as pd
from datetime import timedelta, datetime

from app.common.utils.date_utils import get_current_local_datetime
from app.common.cloudlogging import log_task_failure, log_task_success

from app.common.gcpsecretmanager import get_secret, secret_to_json
from app.common.constant import (
    TIMEZONE, YYYY_MM_DD_HH_MM_SS_FF_FORMAT, FREQUENCY,
    SECRET_DETAIL, SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG, SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG_VERSION_ID
)

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


def execute_demo_pipeline(options, from_time, to_time):
    """
    function to execute pipeline
    @param pipeline: obj.
    @return: none.
    """
    migration_list = list()

    all_transfers_query_string = """
        SELECT * EXCEPT(id) 
        FROM `internal-blockchain-indexed.ethereum.all_transfers`
        WHERE txn_ts >= UNIX_SECONDS("{from_time}")
          AND txn_ts < UNIX_SECONDS("{to_time}")
    """

    delete_query = """
        DELETE FROM soc_data_eth_db.ethereum_transfer_tab 
        WHERE txn_ts >= UNIX_TIMESTAMP(%s)
          AND txn_ts < UNIX_TIMESTAMP(%s)
    """

    all_transfers_cloudsql_table_name = "ethereum_transfer_tab"

    start = get_current_local_datetime(
        timezone=TIMEZONE,
        datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
    )

    log_task_success(
        payload={
            "message": f"Getting secret information of MySQL connection",
            "detail": {
                "project_id": PROJECT_ID,
                "secret_name": SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG,
                "secret_version": SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG_VERSION_ID,
            }
        },
        start_time=start
    )
    try:
        secret_uri = SECRET_DETAIL.format(
            PROJECT_ID,
            SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG,
            SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG_VERSION_ID
        )

        mysql_connection = secret_to_json(
            secret_payload=get_secret(
            {
                "name": secret_uri
            }
        )
        )
    except Exception as e:
        log_task_failure(
            payload={
                "message": f"Failed to get secret information of MySQL connection from {secret_uri}",
                "detail": {
                    "project_id": PROJECT_ID,
                    "secret_name": SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG,
                    "secret_version": SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG_VERSION_ID,
                    "error_message": str(e)
                }
            }
        )

        raise e



    log_task_success(
        payload={
            "message": f"Successful to get secret information of MySQL connection from {secret_uri}",
            "detail": {
                "project_id": PROJECT_ID,
                "secret_name": SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG,
                "secret_version": SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG_VERSION_ID,
                "host": mysql_connection["host"],
                "user": mysql_connection["user"],
                "password": ("*****" + mysql_connection["user"][5:]),
                "database": mysql_connection["database"]
            }
        },
        start_time=start
    )

    try:
        start_time = from_time
        time_range = pd.date_range(from_time, to_time, freq=FREQUENCY)
        for idx, _single_time in enumerate(time_range):
            if idx == 0:
                start_time = _single_time.strftime(YYYY_MM_DD_HH_MM_SS_FF_FORMAT)
                continue

            all_transfers_migration_profile = MigrationProfileModel(
                query_string=all_transfers_query_string.format(
                    from_time=start_time,
                    to_time=_single_time.strftime(YYYY_MM_DD_HH_MM_SS_FF_FORMAT)
                ),
                cloudsql_table_name=all_transfers_cloudsql_table_name,
                delete_query=delete_query,
                from_time=start_time,
                to_time=_single_time.strftime(YYYY_MM_DD_HH_MM_SS_FF_FORMAT),
                mysql_connection=mysql_connection
            )

            migration_list.append(all_transfers_migration_profile)

            end_time = _single_time.strftime(YYYY_MM_DD_HH_MM_SS_FF_FORMAT)

            log_task_success(
                payload={
                    "message": f"Successful to build BigQuery query profiles from {start_time} to {end_time}",
                    "detail": {
                        "from_time": from_time,
                        "to_time": to_time,
                        "start_time": start_time,
                        "end_time": end_time
                    }
                },
                start_time=start
            )

            start_time = end_time

            if idx == len(time_range) - 1:
                break

        beam_profiles = convert_to_beam_profiles(migration_list)

    except Exception as e:
      log_task_failure(
          payload={
              "message": f"Failed to build migration profiles",
              "detail": {
                  "from_time": from_time,
                  "to_time": to_time,
                  "error_message": str(e)
              }
          }
      )

    log_task_success(
        payload={
            "message": f"Successful to build dataflow pipeline profiles",
            "detail": {
                "from_time": from_time,
                "to_time": to_time
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
            "message": f"Beginning the pipeline to ingest data from BigQuery to Cloud SQL with from_time {from_time} and to_time {to_time}",
            "detail": {
                "from_time": from_time,
                "to_time": to_time
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