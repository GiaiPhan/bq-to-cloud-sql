# import os
import time
# import requests
import datetime as dt
# import pytz

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable

from utils.email import _send_successful_email_notification

import googleapiclient.discovery


query_base = """
    SELECT 
        trans.`hash` as txn_hash, 
        trans.block_hash as block_hash, 
        trans.transaction_type as transfer_type, 
        trans.transaction_index as ref_index, 
        trans.block_number as block_number, 
        trans.from_address as from_address, 
        trans.to_address as to_address, 
        cont.address as contract_address, 
        trans.value as quantity, 
        trans.block_timestamp as txn_ts, 
        tok.symbol as token_id, 
        trans.block_hash as operator_address, 
        EXTRACT(HOUR from trans.block_timestamp) as update_time, 
    FROM `bigquery-public-data.crypto_ethereum.transactions`  trans 
    INNER JOIN `bigquery-public-data.crypto_ethereum.contracts` cont 
        ON trans.block_number = cont.block_number 
    LEFT JOIN `bigquery-public-data.crypto_ethereum.tokens` tok 
        ON trans.block_number = tok.block_number 
    WHERE TIMESTAMP_TRUNC(trans.block_timestamp, DAY) >= TIMESTAMP({query_date})"
"""




with DAG(
    dag_id="export_bq_to_cloudsql",
    tags=["trigger"],
    start_date=dt.datetime(2023, 12, 17),
    schedule_interval=None,
    params={
        "timezone": "America/Mexico_City",
        "bucket":"int-data-ct-spotonchain-bq-cloudsql-temp",
        "projectid":"int-data-ct-spotonchain",
        "prefix":"ethereum_transfer_tab_",
        "table_folder":"ethereum_transfer_tab",
        "query_date": "2018-01-01",
        "instance":"spotonchain-test",
        "databaseschema":"spotonchain_db",
        "importtable":"ethereum_transfer_tab",
        "listResult": {"nextPageToken":""}
    },

) as dag:
    @task()
    def export_bq_table(bucket, prefix, table_folder, projectid, query_date):
            
        from google.cloud import bigquery
        client = bigquery.Client(project=projectid)

        table_folder = table_folder + "_from_" + str(query_date)
        query_str = "EXPORT DATA OPTIONS( uri='gs://" + bucket + "/" + table_folder + "/" + prefix + "*.csv', format='CSV', overwrite=true,header=false) AS " + query_base.format(query_date)
        
        # Perform a query.
        query_job = client.query(query_str)  # API request
        rows = query_job.result()  # Waits for query to finish
    
    @task()
    def list_gcs_files(bucket, table_folder, prefix, upstream_task, delimiter="/"):
        from google.cloud import storage
        """Lists all the blobs in the bucket."""
        storage_client = storage.Client()

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(bucket, prefix=table_folder + "/" + prefix, delimiter=delimiter)
        
        return [blob.name for blob in blobs]

    @task()
    def import_gcs_to_cloudsql(bucket, prefix, table_folder, projectid, instance, databaseschema, importtable, gcs_files):
        # Construct the service object for the interacting with the Cloud SQL Admin API.
        service = googleapiclient.discovery.build('sqladmin', 'v1beta4')
        # sql_instance = service.instances().get(project=projectid, instance=instance).execute()
        
        # tmp = gcs_files[0:2]
        failed_files = []

        for file_name in gcs_files:
            cxt = {
                "importContext": {
                    "uri":f"gs://" + bucket + "/" + file_name,
                    "database": databaseschema,
                    "fileType": "CSV",
                    "csvImportOptions": {
                        "table": importtable
                    }
                }
            }
            request = service.instances().import_(project=projectid, instance=instance, body=cxt)
            response = request.execute()

            oper_id = response["name"]
            status = response["status"]
            while status != "DONE":
                if status == "SQL_OPERATION_STATUS_UNSPECIFIED":
                    failed_files.append(file_name)
                    break
                request = service.operations().get(project=projectid, operation=oper_id)
                response = request.execute()
                status = response["status"]

                time.sleep(3)

        return {"status": "", "failed_files": failed_files}

    @task()
    def delete_temporary_gcs_files(bucket, table_folder, query_date, upstream_task):
        from google.cloud import storage

        table_folder = table_folder + "_from_" + str(query_date)

        client = storage.Client()
        bucket = client.get_bucket(bucket)
        # list all objects in the directory
        blobs = bucket.list_blobs(prefix=table_folder)
        for blob in blobs:
            blob.delete()
            
    
    export_bq = export_bq_table(bucket="{{ params.bucket }}", \
                        prefix="{{ params.prefix }}", \
                        table_folder="{{ params.table_folder }}", \
                        projectid="{{ params.projectid }}", \
                        query_date="{{ params.query_date }}"
                    )

    list_files = list_gcs_files(bucket="{{ params.bucket }}", \
                       table_folder="{{ params.table_folder }}", \
                       prefix="{{ params.prefix }}",
                       upstream_task=export_bq
                       )
    
    imp_operation = import_gcs_to_cloudsql(bucket="{{ params.bucket }}", \
                               prefix="{{ params.prefix }}", \
                               table_folder="{{ params.table_folder }}", \
                               projectid="{{ params.projectid }}", \
                               instance="{{ params.instance }}", \
                               databaseschema="{{ params.databaseschema }}", \
                               importtable="{{ params.importtable }}",
                               gcs_files=list_files)
    
    delete_temporary_gcs_files(bucket="{{ params.bucket }}", \
                               table_folder="{{ params.table_folder }}",
                               query_date="{{ params.query_date }}",
                               upstream_task=imp_operation)

globals()[dag.dag_id] = dag
