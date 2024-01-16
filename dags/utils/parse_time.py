import pytz
import pandas as pd
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


def get_run_time_params(interval):
    datetime_format = "%Y-%m-%d %H:%M:%S.%f"
    to_time = get_current_local_datetime("UTC", datetime_format)
    to_time = pd.to_datetime(to_time).floor('30T').to_pydatetime()
    from_time = to_time - timedelta(hours=0, minutes=interval)
    from_time = pd.to_datetime(from_time).floor('5T').to_pydatetime().strftime(datetime_format)
    to_time = to_time.strftime(datetime_format)
    
    return from_time, to_time
