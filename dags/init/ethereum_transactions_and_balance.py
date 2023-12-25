from datetime import timedelta
import os
from os.path import dirname
import time
# import requests
import datetime as dt
# import pytz

from airflow import DAG
from airflow.decorators import dag, task

from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

from dags.custom_package.cloudace_custom_package.cloudace_operators.build_dataflow_body_operator import CloudAceBuildDataflowBodyOperator

# from dags.utils.email import _send_successful_email_notification
# from dags.utils.ca_utils import MySQL, BigQuery
from dags.utils.config_utils import CloudAceConfigUtilsYaml

# import pandas as pd


config = CloudAceConfigUtilsYaml("/opt/airflow/dags/dags_config/eth_migration_pipeline_source_config.yaml")
config_body = config.config_body
project_id = config_body["project_id"]
location = config_body["location"]
gcp_conn_id=config_body["gcp_conn_id"]
dataflow_training_pipeline = config_body["dataflow_pipeline"]
default_args = config_body["default_args"]


with DAG(
    dag_id="ethereum_transactions_and_balance",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 25),
    schedule_interval=None,
    params={
        "from_date": "2023-12-21",
        "to_date": "2023-12-22",
        "listResult": {"nextPageToken":""}
    },

) as ethereum_transactions_and_balance:

    """ Step 1: Dummy start task """
    start_task = EmptyOperator(
        dag=ethereum_transactions_and_balance,
        task_id='start_task'
    )
    print("{{ params.from_date }}", "{{ params.to_date }}")

    """ Step 2.1: Build Dataflow Body """
    build_ingest_dataflow_body = CloudAceBuildDataflowBodyOperator(
        dag=ethereum_transactions_and_balance,
        task_id='build_ingest_dataflow_body',
        job_name_prefix="eth-migrate",
        from_date="{{ task.params.from_date }}",
        to_date="{{ task.params.to_date }}",
        dataflow_config=dataflow_training_pipeline
    )

    """ Step 2.2: Create Dataflow Job """
    create_dataflow_job = DataflowStartFlexTemplateOperator(
        dag=ethereum_transactions_and_balance,
        task_id="create_dataflow_job",
        # gcp_conn_id=gcp_conn_id,
        location=location,
        wait_until_finished=True,
        body=build_ingest_dataflow_body.output,
        project_id=project_id,
        execution_timeout=timedelta(hours=1)
    )
    
    """ Step 3: Dummy end task """
    end_task = EmptyOperator(
        dag=ethereum_transactions_and_balance,
        task_id='end_task'
    )

    """ Set up dependencies """

    start_task >>  build_ingest_dataflow_body >> create_dataflow_job >> end_task


globals()[ethereum_transactions_and_balance.dag_id] = ethereum_transactions_and_balance
