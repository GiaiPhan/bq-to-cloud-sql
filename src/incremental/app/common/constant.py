# config
PROJECT_ID = "internal-blockchain-indexed"

# migration profile
QUERY_STRING = "query_string"
CLOUDSQL_TABLE_NAME = "cloudsql_table_name"
CLOUDSQL_CONNECTION = "mysql_connection"

# datetime
# TIMEZONE = "Asia/Ho_Chi_Minh"
TIMEZONE = "UTC"
YYYY_MM_DD_HH_MM_SS_FF_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
FREQUENCY = "30min"

# cloud logging
INFO_SEVERITY = "INFO"
ERROR_SEVERITY = "ERROR"
FAILED_STATUS = "Failed"
COMPLETED_STATUS = "Completed"
PIPELINE_ORCHESTRATION_ERROR_LOG_NAME = "bq_to_sql_error_logs"
PIPELINE_ORCHESTRATION_INFO_LOG_NAME = "bq_to_sql_info_logs"


# gcp secret manager
SECRET_DETAIL = "projects/{0}/secrets/{1}/versions/{2}"
SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG = "spotonchain_database_secret"
SECRET_BQ_TO_SQL_PIPELINE_EXECUTION_CONFIG_VERSION_ID = "latest"