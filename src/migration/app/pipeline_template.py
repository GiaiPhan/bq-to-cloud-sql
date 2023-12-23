import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from app.config.application_config import SING_LOCATION
from app.pipeline.demo_pipeline import execute_demo_pipeline


if __name__ == "__main__":
    # build pipeline option
    options = PipelineOptions(
        save_main_session=False,
        streaming=False,
        direct_num_workers=2,
        runner="DataflowRunner",
        number_of_worker_harness_threads=10,
        experiments=["no_use_multiple_sdk_containers", "use_runner_v2"],
        max_num_workers=4,
        region=SING_LOCATION,
        setup_file='/dataflow/template/setup.py'
    )

    pipeline = beam.Pipeline(options=options)

    execute_demo_pipeline(
        pipeline=pipeline
    )

    pipeline.run()
