import os
import time
import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

from custom_package.cloudace_custom_package.cloudace_operators.build_dataflow_body_operator import CloudAceBuildDataflowBodyOperator, GetCustomParam

# from utils.email import _send_successful_email_notification
# from utils.ca_utils import MySQL, BigQuery
from utils.config_utils import CloudAceConfigUtilsYaml



# Parse configurations
config = CloudAceConfigUtilsYaml("/opt/airflow/dags/dags_config/eth_incremental_pipeline_source_config.yaml")
config_body = config.config_body
project_id = config_body["project_id"]
location = config_body["location"]
gcp_conn_id=config_body["gcp_conn_id"]
dataflow_training_pipeline = config_body["dataflow_pipeline"]
default_args = config_body["default_args"]
timeout = config_body["timeout"]
schedule_interval = config_body["schedule_interval"]
start_date = config.get_start_date()
tags = config_body["tags"]



# Initialize dag instance
with DAG(
    dag_id="incremental_all_transfer",
    tags=tags,
    start_date=start_date,
    schedule_interval=schedule_interval,
    default_args=default_args,
    params={
        "interval": Param(60, enum=[30, 60, 120]),
    }
) as ethereum_transactions:

    """ Step 1: Dummy start task """
    start_task = EmptyOperator(
        dag=ethereum_transactions,
        task_id='start_task'
    )

    """ Step 2.1: Build Dataflow Body """
    build_ingest_dataflow_body = CloudAceBuildDataflowBodyOperator(
        dag=ethereum_transactions,
        task_id='build_ingest_dataflow_body',
        job_name_prefix="eth-incre",
        dataflow_config=dataflow_training_pipeline
    )

    """ Step 2.2: Create Dataflow Job """
    create_dataflow_job = DataflowStartFlexTemplateOperator(
        dag=ethereum_transactions,
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
        dag=ethereum_transactions,
        task_id='end_task'
    )

    """ Set up dependencies """
    start_task >> build_ingest_dataflow_body >> create_dataflow_job >> end_task


globals()[ethereum_transactions.dag_id] = ethereum_transactions
