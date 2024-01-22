#!/bin/bash
gcloud dataflow flex-template build \
"gs://bq-to-cloud-sql-template/transaction-pipeline-template.json" \
--image "us-east5-docker.pkg.dev/internal-blockchain-indexed/dataflow-container/transaction-pipeline-template-container:latest" \
--sdk-language "PYTHON" \
--metadata-file "transaction-pipeline-metadata.json"