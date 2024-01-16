#!/bin/bash
gcloud dataflow flex-template build \
"gs://bq-to-cloud-sql-template/incremental-pipeline-template.json" \
--image "us-east5-docker.pkg.dev/internal-blockchain-indexed/dataflow-container/pipeline-template-incremental-container:latest" \
--sdk-language "PYTHON" \
--metadata-file "incremental-pipeline-metadata.json"