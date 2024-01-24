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
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

from custom_package.cloudace_custom_package.cloudace_operators.build_dataflow_body_operator import CloudAceBuildDataflowBodyOperator

# from utils.email import _send_successful_email_notification
# from utils.ca_utils import MySQL, BigQuery
from utils.config_utils import CloudAceConfigUtilsYaml

# import pandas as pd


config = CloudAceConfigUtilsYaml("/opt/airflow/dags/dags_config/eth_migration_transaction_source_config.yaml")
config_body = config.config_body
project_id = config_body["project_id"]
location = config_body["location"]
gcp_conn_id=config_body["gcp_conn_id"]
dataflow_training_pipeline = config_body["dataflow_pipeline"]
default_args = config_body["default_args"]
timeout = config_body["timeout"]
tags = config_body["tags"]



with DAG(
    dag_id="test",
    tags=tags,
    start_date=dt.datetime(2023, 12, 25),
    schedule_interval=None,
    default_args=default_args,
    params={
        "from_date": "2023-12-21",
        "to_date": "2023-12-22"
    },

) as dag_transaction:

    """ Step 1: Dummy start task """
    start_task = EmptyOperator(
        task_id='start_task'
    )

    def test(proj, name, tmp):
        print(proj + " " + name)
        print(tmp)
        pass

    end_task = PythonOperator(
        task_id="test",
        python_callable=test,
        op_kwargs={"proj": "{{ params.from_date }}", "name": "{{ params.to_date }}", "tmp": "{{ params.from_date}}_{{ params.to_date}}"},
    )

    start_task >>  end_task


globals()[dag_transaction.dag_id] = dag_transaction
