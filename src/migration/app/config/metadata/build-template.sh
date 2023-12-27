#!/bin/bash
gcloud dataflow flex-template build \
"gs://int-data-ct-spotonchain-dataflow-template/optimize-pipeline-template.json" \
--image "asia-southeast1-docker.pkg.dev/int-data-ct-spotonchain/dataflow-container/pipeline-template-optimize-container:latest" \
--sdk-language "PYTHON" \
--metadata-file "demo-pipeline-metadata.json"