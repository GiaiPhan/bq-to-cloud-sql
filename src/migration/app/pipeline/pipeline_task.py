import apache_beam as beam
from app.database.mysql.mysql_connector import MySQL
from app.database.bigquery.bigquery_connector import BigQuery
from app.config.application_config import US_LOCATION, PROJECT_ID, CHUNK_SIZE
from app.common.constant import (
    QUERY_STRING, CLOUDSQL_TABLE_NAME, CLOUDSQL_CONNECTION,
    TIMEZONE, YYYY_MM_DD_HH_MM_SS_FF_FORMAT,
    SECRET_DETAIL, SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG, SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG_VERSION_ID
)
from app.common.utils.date_utils import get_current_local_datetime
from app.common.cloudlogging import log_task_failure, log_task_success



class LoadFromBigQueryToCloudSQL(beam.DoFn):
    def process(self, data):
        # def delete_from_mysql(delete_query, from_date, to_date):
        #     from datetime import datetime, timedelta

        #     mysql_object = MySQL()
        #     date_plus_one = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
        #     date_plus_one = date_plus_one.strftime("%Y-%m-%d")

        #     mysql_object.execute(delete_query, (from_date, date_plus_one))

        # def truncate_mysql_table(cloudsql_table_name):

        #     mysql_object = MySQL()
        #     mysql_object.truncate(cloudsql_table_name)


        def load_from_bigquery_to_cloudsql(mysql_connection, query_string, cloudsql_table_name, from_date, to_date):
            """
            function to load data from bigquery into cloudsql
            @param query_string: str.
            @param cloudsql_table_name: str.
            @return: none.
            """
            import pandas as pd

            bq_object = BigQuery()
            bq_connection = bq_object.get_connection(US_LOCATION, PROJECT_ID)

            try:
                cloudsql_object = MySQL(
                    host=mysql_connection["host"],
                    user=mysql_connection["user"],
                    password=mysql_connection["password"],
                    database=mysql_connection["database"],
                )
            except Exception as e:
                log_task_failure(
                    payload={
                        "message": f"Failed to create MySQL connection",
                        "detail": {
                            "error_message": str(e)
                        }
                    }
                )

                raise e

            chunk_sum = 0
            
            try:
                for idx, chunk in enumerate(pd.read_sql(sql=query_string, con=bq_connection, chunksize=CHUNK_SIZE)):
                    start = get_current_local_datetime(
                        timezone=TIMEZONE,
                        datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
                    )

                    try:
                        cloudsql_object.write(
                            df=chunk,
                            table_name=cloudsql_table_name,
                            chunk_size=CHUNK_SIZE
                        )
                        chunk_sum += len(chunk)
                    except Exception as e:
                        log_task_failure(
                            payload={
                                "message": f"Failed to load {len(chunk)} rows into table {cloudsql_table_name} CloudSQL",
                                "chunk_index": idx,
                                "total_row_loaded": str(chunk_sum),
                                "detail": {
                                    "cloudsql_table_name": cloudsql_table_name,
                                    "from_date": from_date,
                                    "to_date": to_date,
                                    "bq_query_string": query_string,
                                    "chunksize": CHUNK_SIZE,
                                    "error_message": str(e)
                                }
                            }
                        )

                        raise e
                    
                    log_task_success(
                        payload={
                            "message": f"Loaded {len(chunk)} rows into table {cloudsql_table_name} CloudSQL",
                            "chunk_index": idx,
                            "total_row_loaded": str(chunk_sum),
                            "detail": {
                                "cloudsql_table_name": cloudsql_table_name,
                                "from_date": from_date,
                                "to_date": to_date,
                                "bq_query_string": query_string,
                                "chunksize": CHUNK_SIZE,
                            }
                        },
                        start_time=start
                    )

            except Exception as e:
                log_task_failure(
                    payload={
                        "message": f"Failed to ingest data from BigQuery to Cloud SQL with from_date {from_date} and to_date {to_date}",
                        "chunk_index": idx,
                        "total_row_loaded": str(chunk_sum),
                        "detail": {
                            "cloudsql_table_name": cloudsql_table_name,
                            "from_date": from_date,
                            "to_date": to_date,
                            "bq_query_string": query_string,
                            "chunksize": CHUNK_SIZE,
                            "error_message": str(e)
                        }
                    }
                )

                raise e


        start = get_current_local_datetime(
            timezone=TIMEZONE,
            datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
        )


        try:
            from_date = data.get("from_date", "")
            to_date = data.get("to_date", "")
            query_string = data.get(QUERY_STRING, "")
            cloudsql_table_name = data.get(CLOUDSQL_TABLE_NAME, "")
            mysql_connection = data.get(CLOUDSQL_CONNECTION, None)

            if not from_date or not to_date or not query_string or not cloudsql_table_name:
                log_task_failure(
                    payload={
                        "message": "Failed to get parse information of Dataflow Pipline",
                        "detail": {
                            "from_date": from_date,
                            "to_date": to_date,
                            "query_string": query_string,
                            "cloudsql_table_name": cloudsql_table_name,
                            "error_message": "Failed to get parse information of Dataflow Pipline"
                        }
                    }
                )

                raise Exception("Failed to get parse information of Dataflow Pipline")
            

            if from_date == "" or to_date == "" or query_string == "" or cloudsql_table_name == "":
                log_task_failure(
                    payload={
                        "message": "Invalid parameters of Dataflow Pipeline",
                        "detail": {
                            "from_date": from_date,
                            "to_date": to_date,
                            "query_string": query_string,
                            "cloudsql_table_name": cloudsql_table_name,
                            "error_message": "Invalid parameters of Dataflow Pipeline"
                        }
                    }
                )

                raise Exception("Invalid parameters of Dataflow Pipeline")


        except Exception as e:
            log_task_failure(
                payload={
                    "message": "Failed to get parse information of Dataflow Pipline",
                    "detail": {
                        "from_date": from_date,
                        "to_date": to_date,
                        "query_string": query_string,
                        "cloudsql_table_name": cloudsql_table_name,
                        "error_message": str(e)
                    }
                }
            )

            raise e

        log_task_success(
            payload={
                "message": f"Beginning to ingest data from BigQuery to Cloud SQL with from_date {from_date} and to_date {to_date}",
                "detail": {
                    "cloudsql_table_name": cloudsql_table_name,
                    "from_date": from_date,
                    "to_date": to_date,
                    "bq_query_string": query_string,
                    "chunksize": CHUNK_SIZE
                }
            },
            start_time=start
        )

        try:
            load_from_bigquery_to_cloudsql(
                mysql_connection=mysql_connection,
                query_string=query_string,
                cloudsql_table_name=cloudsql_table_name,
                from_date=from_date,
                to_date=to_date
            )

        except Exception as e:
            log_task_failure(
                payload={
                    "message": f"Failed to ingest data from BigQuery to Cloud SQL with from_date {from_date} and to_date {to_date}",
                    "detail": {
                        "cloudsql_table_name": cloudsql_table_name,
                        "from_date": from_date,
                        "to_date": to_date,
                        "bq_query_string": query_string,
                        "chunksize": CHUNK_SIZE,
                        "error_message": str(e)
                    }
                }
            )

            raise e


        log_task_success(
            payload={
                "message": f"Sucesssful to ingest data from BigQuery to Cloud SQL with from_date {from_date} and to_date {to_date}",
                "detail": {
                    "cloudsql_table_name": cloudsql_table_name,
                    "from_date": from_date,
                    "to_date": to_date,
                    "bq_query_string": query_string,
                    "chunksize": CHUNK_SIZE
                }
            },
            start_time=start
        )