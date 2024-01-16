from sqlalchemy import create_engine
from google.cloud import bigquery
from app.config.application_config import PROJECT_ID


class BigQuery:
    def __init__(self, project=PROJECT_ID):
        self.project_id = project
        self.client = bigquery.Client(project=project)

    def get_connection(self, location: str, dataset_id: str = ""):
        database_connection = create_engine(
            self._build_bq_connection_string(dataset_id),
            location=location,
            arraysize=5000,
            list_tables_page_size=50
        )
        return database_connection

    def _build_bq_connection_string(self, dateset_id: str = ""):
        if dateset_id and len(dateset_id) > 0:
            connection_string = "bigquery://{0}/{1}?user_supplied_client=True".format(
                self.project_id,
                dateset_id
            )
            return connection_string
        else:
            connection_string = "bigquery://{0}?user_supplied_client=True".format(
                self.project_id
            )
            return connection_string
