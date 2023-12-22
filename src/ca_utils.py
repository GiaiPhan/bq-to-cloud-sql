import sqlalchemy
from sqlalchemy import create_engine

from pandas.io import sql
from google.cloud import bigquery



class MySQL:
    temp_update_table = 'temp_update'

    def __init__(self, 
                 host="34.124.221.251", user="userdemo", 
                 password="userdemo@?C&_:cNvt}{P(%;1", database="spotonchain_demo"
    ):
        try:
            self.db = create_engine('mysql://{user}:{password}@{host}/{database}'.format(
                host=host, user=user, 
                password=password, database=database
            )).connect()

        except Exception as e:
            print(e)

    

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
        self.client = bigquery.Client(project=project)

    def query_to_dataframe(self, query):
        df = self.client.query(query).to_dataframe()

        return df
