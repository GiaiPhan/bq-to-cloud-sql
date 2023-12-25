from typing import Sequence
from airflow.models import BaseOperator
from airflow.utils.context import Context


class CloudAceBuildDataflowBodyOperator(BaseOperator):

    template_fields: Sequence[str] = (
        "dataflow_config",
    )

    def __init__(self, job_name_prefix: str,  dataflow_config: dict, from_date: str, to_date: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.job_name_prefix = job_name_prefix
        self.dataflow_config = dataflow_config
        self.from_date = from_date
        self.to_date = to_date


    def execute(self, context: Context) -> dict:
        ti = context["ti"]
        run_id = context.get("run_id")
        ds_nodash = context.get("ds_nodash")
        dag_run = context.get("dag_run")
        execution_date = dag_run.logical_date

        self.log.info(f"run_id: {run_id}")
        self.log.info(f"execution_date: {execution_date}")

        # Get upstream task instance
        upstream_task_ids = self.upstream_task_ids
        upstream_task_attempt_list = [{
            "task_id": task_id,
            "attempt_number": dag_run.get_task_instance(task_id=task_id).prev_attempted_tries
        } for task_id in upstream_task_ids]
        
        max_attemps = max(upstream_task_attempt_list, key=lambda x:x["attempt_number"])

        self.log.info(f"Upstream task attempt list: {upstream_task_attempt_list}")
        self.log.info(f"Max attempt: {max_attemps}")
        self.log.info(f"Task instance prev_attempted_tries: {ti.prev_attempted_tries}")

        # If prev_attempted_tries is 0 or 1 then it is the initial run
        if max_attemps["attempt_number"] in (0, 1) or ti.prev_attempted_tries == 0:
            rerun_list = [t["task_id"] for t in upstream_task_attempt_list]
            self.log.info("Initial run, no upstream retry, pass all tables to dataflow")
            self.log.info(f"Initial list: {rerun_list}")
        # Else it is a retry
        else:
            rerun_list = [t["task_id"] for t in upstream_task_attempt_list if t["attempt_number"] == max_attemps["attempt_number"]]
            self.log.info("Retry run, pass selected tables to dataflow")
            self.log.info(f"Retry list: {rerun_list}")

        dataflow_body = dict()

        dataflow_body["launchParameter"] = {
            "jobName": f"{self.job_name_prefix}-{ds_nodash}",
            "containerSpecGcsPath": self.dataflow_config['template_path'],
            "environment": {
                # "tempLocation": self.dataflow_config['temp_location'],
                # "ipConfiguration": "WORKER_IP_PRIVATE",
                "machineType": self.dataflow_config["machine_type"] if "machine_type" in self.dataflow_config.keys() else "n1-standard-2",
                "numWorkers": 2,
                "maxWorkers": 4
            },
            "parameters": {
                "from_date": self.from_date,
                "to_date": self.to_date
            }
        }

        return dataflow_body