import pytz
from datetime import datetime, date, timedelta


def get_current_local_datetime(timezone: str, datetime_format: str) -> str:
    """
    function to get current local datetime
    @param timezone: str.
    @param datetime_format: str.
    @return: local_datetime: str.
    """
    time_zone = pytz.timezone(timezone)
    local_datetime = datetime.now(tz=time_zone)
    local_datetime = local_datetime.strftime(datetime_format)

    return local_datetime


def get_update_time(timezone: str) -> datetime:
    """
    function to get update time
    @param timezone: str.
    @return: local_datetime: str.
    """
    time_zone = pytz.timezone(timezone)
    local_datetime = datetime.now(tz=time_zone)

    return local_datetime


def convert_date_string_to_date(date_string: str, date_format: str) -> date:
    """
    function to convert date string to date
    @param date_string: str.
    @param date_format: str.
    @return: local_datetime: str.
    """
    return datetime.strptime(date_string, date_format).date()


def get_unique_current_datetime(timezone: str, datetime_format: str) -> str:
    """
    function to get unique current datetime
    @param timezone: str.
    @param datetime_format: str.
    @return: unique_datetime: str.
    """
    time_zone = pytz.timezone(timezone)
    today = datetime.now(tz=time_zone)
    unique_datetime = today.strftime(datetime_format)
    return unique_datetime


def get_total_duration(start_datetime: str, end_datetime: str, datetime_format: str) -> str:
    """
    function to get total duration
    @param start_datetime: str.
    @param end_datetime: str.
    @param datetime_format: str.
    @return: total_duration: str.
    """
    start_time = datetime.strptime(start_datetime, datetime_format)
    end_time = datetime.strptime(end_datetime, datetime_format)
    duration_in_seconds = (end_time - start_time).total_seconds()
    total_duration = str(round(duration_in_seconds / 60, 2)) + " minutes"
    return total_duration


def generate_future_datetime(local_datetime_string: str, datetime_format: str, minutes: float) -> datetime:
    """
    function to generate future datetime
    @param local_datetime_string: str.
    @param datetime_format: str.
    @param minutes: float.
    @return: future_datetime: datetime.
    """
    execution_datetime = datetime.strptime(local_datetime_string, datetime_format)
    future_datetime = execution_datetime + timedelta(minutes=minutes)

    return future_datetime
