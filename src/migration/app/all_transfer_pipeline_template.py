import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from app.config.application_config import COLOMBUS_LOCATION
from app.pipeline.all_transfer_pipeline import execute_demo_pipeline


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--from_date', required=True, help='Environment that Migration Pipeline run on')
    parser.add_argument('--to_date', required=True, help='Environment that Migration Pipeline run on')

    known_args, pipeline_args = parser.parse_known_args()

    from_date = known_args.from_date
    to_date = known_args.to_date

    # build pipeline option
    options = PipelineOptions(
        save_main_session=False,
        streaming=False,
        direct_num_workers=2,
        runner="DataflowRunner",
        number_of_worker_harness_threads=10,
        experiments=["no_use_multiple_sdk_containers", "use_runner_v2"],
        max_num_workers=4,
        region=COLOMBUS_LOCATION,
        setup_file='/dataflow/template/setup.py'
    )

    execute_demo_pipeline(
        options=options,
        from_date=from_date,
        to_date=to_date
    )