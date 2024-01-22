#!/bin/bash
gcloud dataflow flex-template build \
"gs://bq-to-cloud-sql-template/all-transfer-pipeline-template.json" \
--image "us-east5-docker.pkg.dev/internal-blockchain-indexed/dataflow-container/all-transfer-pipeline-template-container:latest" \
--sdk-language "PYTHON" \
--metadata-file "all-transfer-pipeline-metadata.json"