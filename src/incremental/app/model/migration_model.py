class MigrationProfileModel:
    def __init__(self, query_string, cloudsql_table_name, delete_query, from_time, to_time, mysql_connection):
        self.query_string = query_string
        self.cloudsql_table_name = cloudsql_table_name
        self.delete_query = delete_query
        self.from_time = from_time
        self.to_time = to_time
        self.mysql_connection = mysql_connection
