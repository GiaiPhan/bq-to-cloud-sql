class MigrationProfileModel:
    def __init__(self, query_string, cloudsql_table_name):
        self.query_string = query_string
        self.cloudsql_table_name = cloudsql_table_name
