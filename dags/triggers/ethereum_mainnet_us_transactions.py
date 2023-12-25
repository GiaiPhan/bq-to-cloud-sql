from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration


@task
def start_task(task_type):
    return f"The {task_type} task has completed."


@task
def end_task(task_type):
    return f"The {task_type} task has completed."


# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
}


@dag(
    dag_id="trigger_init_ethereum_mainnet_us_transactions",
    start_date=datetime(2023, 12, 21),
    max_active_runs=1,
    schedule=None,
    default_args=default_args,
    catchup=False,
    params={
        "projectid": "int-data-ct-spotonchain",
        "from_date": "2023-12-17",
        "to_date": "2023-12-18",
        "partition_field": "block_timestamp",
        "databaseschema":"spotonchain",
        "mysqltable": "transactions",
        "table_filter_date": "block_timestamp",
        "num_days_per_trigger": 10,
        "listResult": {"nextPageToken":""}
    },
)
def trigger_dagrun_dag():
    # import pandas module
    import pandas as pd
    
    first_day = "{{ params.from_date }}"
    last_day = "{{ params.to_date }}"


    date_range = pd.date_range(start=first_day, end=last_day)
    
    if first_day == last_day:
        trigger_dependent_dag = TriggerDagRunOperator(
            task_id="trigger_init_ethereum_mainnet_us_transactions_task",
            trigger_dag_id="init_ethereum_mainnet_us_transactions",
            wait_for_completion=True,
            deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
            conf={
                "from_date": first_day, 
                "to_date": last_day,
                "partition_field":"{{ params.partition_field }}",
                "databaseschema":"{{ params.databaseschema }}",
                "mysqltable":"{{ params.mysqltable }}",
                "table_filter_date":"{{ params.table_filter_date }}"
            },
        )

        start_task("starting") >> trigger_dependent_dag >> end_task("ending")

    else:


        for idx, _date in enumerate(date_range[::3]):
            if idx == 0:
                continue

            trigger_dependent_dag = TriggerDagRunOperator(
                task_id="trigger_init_ethereum_mainnet_us_transactions_task",
                trigger_dag_id="init_ethereum_mainnet_us_transactions",
                wait_for_completion=True,
                deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
                conf={
                    "from_date": first_day, 
                    "to_date": _date,
                    "partition_field":"{{ params.conf['partition_field'] }}",
                    "databaseschema":"{{ params.conf['databaseschema'] }}",
                    "mysqltable":"{{ params.conf['mysqltable'] }}",
                    "table_filter_date":"{{ params.conf['table_filter_date'] }}"
                },
            )

            start_task("starting") >> trigger_dependent_dag >> end_task("ending")

            first_day = _date


trigger_dagrun_dag()