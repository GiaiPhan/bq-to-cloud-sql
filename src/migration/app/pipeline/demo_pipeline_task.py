import apache_beam as beam
from app.database.mysql.mysql_connector import MySQL
from app.database.bigquery.bigquery_connector import BigQuery
from app.config.application_config import US_LOCATION, PROJECT_ID, CHUNK_SIZE
from app.common.constant import QUERY_STRING, CLOUDSQL_TABLE_NAME


class LoadFromBigQueryToCloudSQL(beam.DoFn):
    def process(self, data):

        def load_from_bigquery_to_cloudsql(query_string, cloudsql_table_name):
            """
            function to load data from bigquery into cloudsql
            @param query_string: str.
            @param cloudsql_table_name: str.
            @return: none.
            """
            import pandas as pd

            bq_object = BigQuery()
            bq_connection = bq_object.get_connection(US_LOCATION, PROJECT_ID)

            cloudsql_object = MySQL()

            try:
                for chunk in pd.read_sql(sql=query_string, con=bq_connection, chunksize=CHUNK_SIZE):
                    cloudsql_object.write(
                        df=chunk,
                        table_name=cloudsql_table_name,
                        chunk_size=CHUNK_SIZE
                    )
                    print(f"Loaded {CHUNK_SIZE} rows into table {cloudsql_table_name} CloudSQL")

            except Exception as e:
                print(f"Failed to loaded data in CloudSQL due to: {str(e)}")
                raise e

        import logging
        from timeit import default_timer as timer

        start = timer()
        load_from_bigquery_to_cloudsql(
            query_string=data.get(QUERY_STRING, ""),
            cloudsql_table_name=data.get(CLOUDSQL_TABLE_NAME, "")
        )
        end = timer()

        logging.info(f'Total time: {round((end - start) / 60, 2)} minutes')
