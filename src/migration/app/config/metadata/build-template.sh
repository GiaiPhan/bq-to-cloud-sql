#!/bin/bash
gcloud dataflow flex-template build \
"gs://internal-blockchain-indexed-dataflow-template/optimize-pipeline-template.json" \
--image "us-east5-docker.pkg.dev/internal-blockchain-indexed/dataflow-container/pipeline-template-optimize-container:latest" \
--sdk-language "PYTHON" \
--metadata-file "demo-pipeline-metadata.json"