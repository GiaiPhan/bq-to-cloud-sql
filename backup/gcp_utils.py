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
