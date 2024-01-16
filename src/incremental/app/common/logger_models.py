class BaseLoggerDetails:
    """
    Define Base logger details
    """
    def __init__(self, project_id: str, status: str, payload: dict):
        self.project_id = project_id
        self.status = status
        self.payload = payload


class DataflowInfoDetails(BaseLoggerDetails):
    """
    Define Dataflow info details
    """
    def __init__(self, start_time, end_time, duration, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.end_time = end_time
        self.duration = duration

    def to_json(self):
        return {
            "project_id": self.project_id,
            "status": self.status,
            "message": self.payload,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration
        }


class DataflowErrorDetails(BaseLoggerDetails):
    """
    Define Dataflow error details
    """
    def __init__(self, failed_time, **kwargs):
        super().__init__(**kwargs)
        self.failed_time = failed_time

    def to_json(self):
        return {
            "project_id": self.project_id,
            "status": self.status,
            "message": self.payload,
            "failed_time": self.failed_time
        }
