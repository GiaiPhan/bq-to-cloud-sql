from sqlalchemy import create_engine


class MySQL:
    def __init__(self, host="10.98.32.3", user="userdemo", 
                 password="m9{f]3o&$IG7kRYh", database="spotonchain_demo"):
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

    def execute(self, query, params=None):
        if not params:
            self.db.execute(query)
        else:
            self.db.execute(query, params)


    def truncate(self, table_name):
        self.db.execute(
            f"TRUNCATE TABLE {table_name};"
        )
