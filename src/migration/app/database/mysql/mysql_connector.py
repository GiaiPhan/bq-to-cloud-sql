from sqlalchemy import create_engine


class MySQL:
    def __init__(self, host="", user="", password="", database=""):
        self.db = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}'.format(
            host=host, user=user,
            password=password, database=database
        )).connect()

    def write(self, df, table_name, chunk_size, if_exists='append'):
        df.to_sql(
            con=self.db,
            name=table_name,
            index=False,
            if_exists=if_exists,
            chunksize=chunk_size
        )
