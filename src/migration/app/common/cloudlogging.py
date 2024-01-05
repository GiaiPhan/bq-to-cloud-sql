from google.cloud import logging as gclogger
from app.common.logger_models import DataflowErrorDetails, DataflowInfoDetails
from app.common.utils.date_utils import get_current_local_datetime, get_total_duration
from app.common.constant import (
    PROJECT_ID, 
    FAILED_STATUS, COMPLETED_STATUS, INFO_SEVERITY, ERROR_SEVERITY, 
    PIPELINE_ORCHESTRATION_ERROR_LOG_NAME, PIPELINE_ORCHESTRATION_INFO_LOG_NAME,
    TIMEZONE, YYYY_MM_DD_HH_MM_SS_FF_FORMAT
)

def get_cloud_logging_client(project_id: str):
    """
    function to get cloud logging client
    @param project_id: str.
    @return: logging_client: obj.
    """
    logging_client = gclogger.Client(project=project_id)

    return logging_client


def get_logger_by_log_name(project_id: str, logger_name: str):
    """
    function to get logger by log name
    @param project_id: str.
    @param logger_name: str.
    @return: cloud_logger: object.
    """
    logging_client = get_cloud_logging_client(project_id)
    cloud_logger = logging_client.logger(logger_name)

    return cloud_logger


def log_task_success(payload, start_time) -> None: 
    end_time = get_current_local_datetime(
        timezone=TIMEZONE,
        datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
    )
    error_log = DataflowInfoDetails(
        project_id=PROJECT_ID,
        status=COMPLETED_STATUS,
        payload=payload,
        start_time=start_time,
        end_time=end_time,
        duration=get_total_duration(
            start_datetime=start_time,
            end_datetime=end_time,
            datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
        )
    )

    error_logger = get_logger_by_log_name(
        project_id=PROJECT_ID,
        logger_name=PIPELINE_ORCHESTRATION_INFO_LOG_NAME
    )
    error_logger.log_struct(
        error_log.to_json(),
        severity=INFO_SEVERITY
    )


def log_task_failure(payload) -> None: 
    failed_time = get_current_local_datetime(
        timezone=TIMEZONE,
        datetime_format=YYYY_MM_DD_HH_MM_SS_FF_FORMAT
    )
    error_log = DataflowErrorDetails(
        project_id=PROJECT_ID,
        status=FAILED_STATUS,
        payload=payload,
        failed_time=failed_time
    )
    error_logger = get_logger_by_log_name(
        project_id=PROJECT_ID,
        logger_name=PIPELINE_ORCHESTRATION_ERROR_LOG_NAME
    )
    error_logger.log_struct(
        error_log.to_json(),
        severity=ERROR_SEVERITY
    )
