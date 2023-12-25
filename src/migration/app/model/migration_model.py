class MigrationProfileModel:
    def __init__(self, query_string, cloudsql_table_name, delete_query, from_date, to_date):
        self.query_string = query_string
        self.cloudsql_table_name = cloudsql_table_name
        self.delete_query = delete_query
        self.from_date = from_date
        self.to_date = to_date
