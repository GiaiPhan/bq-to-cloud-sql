#!/bin/bash
gcloud dataflow flex-template build \
"gs://bq-to-cloud-sql-template/block-pipeline-template.json" \
--image "us-east5-docker.pkg.dev/internal-blockchain-indexed/dataflow-container/block-pipeline-template-container:latest" \
--sdk-language "PYTHON" \
--metadata-file "block-pipeline-metadata.json"