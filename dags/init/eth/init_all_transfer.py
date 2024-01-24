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

from custom_package.cloudace_custom_package.cloudace_operators.build_dataflow_body_operator import CloudAceBuildDataflowBodyOperator, GetCustomParam

# from utils.email import _send_successful_email_notification
# from utils.ca_utils import MySQL, BigQuery
from utils.config_utils import CloudAceConfigUtilsYaml

# import pandas as pd


config = CloudAceConfigUtilsYaml("/opt/airflow/dags/dags_config/eth_migration_all_transfer_source_config.yaml")
config_body = config.config_body
project_id = config_body["project_id"]
location = config_body["location"]
gcp_conn_id=config_body["gcp_conn_id"]
dataflow_training_pipeline = config_body["dataflow_pipeline"]
default_args = config_body["default_args"]
timeout = config_body["timeout"]
tags = config_body["tags"]

with DAG(
    dag_id="init_all_transfer",
    tags=tags,
    start_date=dt.datetime(2023, 12, 25),
    default_args=default_args,
    schedule_interval=None,
    params={
        "from_date": "2023-12-21",
        "to_date": "2023-12-22",
    },

) as dag_all_transfer:

    """ Step 1: Dummy start task """
    start_task = EmptyOperator(
        task_id='start_task'
    )

    custom_param = GetCustomParam(
        task_id="get_custom_param",
        from_date="{{ params.from_date }}",
        to_date="{{ params.to_date }}"
    )

    """ Step 2.1: Build Dataflow Body """
    build_ingest_dataflow_body = CloudAceBuildDataflowBodyOperator(
        task_id='build_ingest_dataflow_body',
        job_name_prefix="eth-all-transfer",
        dataflow_config=dataflow_training_pipeline
    )

    """ Step 2.2: Create Dataflow Job """
    create_dataflow_job = DataflowStartFlexTemplateOperator(
        task_id="create_dataflow_job",
        # gcp_conn_id=gcp_conn_id,
        location=location,
        wait_until_finished=True,
        body=build_ingest_dataflow_body.output,
        project_id=project_id,
        execution_timeout=timedelta(hours=timeout)
    )
    
    """ Step 3: Dummy end task """
    end_task = EmptyOperator(
        task_id='end_task'
    )

    """ Set up dependencies """

    start_task >> custom_param >> build_ingest_dataflow_body >> create_dataflow_job >> end_task


globals()[dag_all_transfer.dag_id] = dag_all_transfer
