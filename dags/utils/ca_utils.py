import sqlalchemy
from sqlalchemy import create_engine

from pandas.io import sql
from google.cloud import bigquery



class MySQL:
    temp_update_table = 'temp_update'

    def __init__(self, 
                 host="34.124.221.251", user="userdemo", 
                 password="m9{f]3o&$IG7kRYh", database="spotonchain_demo"
    ):
        self.db = create_engine('mysql://{user}:{password}@{host}/{database}'.format(
            host=host, user=user, 
            password=password, database=database
        )).connect()

    

    def execute(self, query, params=None):
        if not params:
            self.db.execute(query)
        else:
            self.db.execute(query, params)


    def write(self, df, table_name, if_exists='append'):
        df.to_sql(
            con=self.db, 
            name=table_name, 
            index=False,
            if_exists=if_exists, 
            chunksize=20000
        )

    def update_by_column(self, df, table_name, primary_key, other_colums, if_exists='append'):
        df.to_sql(
            con=self.db, 
            name=self.temp_update_table, 
            index=False,
            if_exists=if_exists, 
            chunksize=20000
        )

        for col in other_colums:
            try:
                sql = f"""
                    UPDATE {table_name} AS f
                    SET f.{col} = t.{col}
                    FROM {self.temp_update_table} AS t
                    WHERE f.{primary_key} = t.{primary_key}
                """

                self.execute(sql)
            except Exception as e:
                print(e)


class BigQuery:

    def __init__(self, project="int-data-ct-spotonchain"):
        self.project_id = project
        self.client = bigquery.Client(project=project)


    def get_connection(self, location: str, dataset_id: str="", chunksize=300000):
        """
        function to get database connection
        @param connection_string: str.
        @param location: str.
        @return: database_connection: obj.
        """
        database_connection = create_engine(
            self._build_bq_connection_string(dataset_id),
            location=location,
            arraysize=5000,
            list_tables_page_size=50
        )
        return database_connection


    def _build_bq_connection_string(self, dateset_id: str=""):
        """
        function to build bigquery connection string
        @param dateset_id: str.
        @return: connection_string: str.
        """
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


    # def query_to_dataframe(self, query):
    #     df = self.client.query(query).to_dataframe()

    #     return df
