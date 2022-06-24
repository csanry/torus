import logging
import logging.config
from typing import NamedTuple

from kfp.components import InputPath, OutputPath
from kfp.v2.dsl import Artifact, Dataset, component


def tfdv_generate_statistics(
    input_data: str, 
    output_path: str, 
    job_name: str, 
    use_dataflow: str,
    project_id: str, 
    region: str, 
    gcs_temp_location: str, 
    gcs_staging_location: str,
    whl_location: str = '', 
) -> NamedTuple('Outputs', [('stats_path', str)]):

    import logging
    import os
    import time

    import tensorflow_data_validation as tfdv
    import tensorflow_data_validation.statistics.stats_impl
    from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                      PipelineOptions,
                                                      SetupOptions,
                                                      StandardOptions)

    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pacific-torus.json"


    logging.getLogger().setLevel(logging.INFO)
    logging.info("output path: %s", output_path)
    logging.info("Building pipeline options")
    
    # Create and set your PipelineOptions.
    options = PipelineOptions()

    if use_dataflow == 'true':
        logging.info("using Dataflow")
        if not whl_location:
            logging.warning('tfdv whl file required with dataflow runner.')
            exit(1)
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = project_id
        google_cloud_options.job_name = '{}-{}'.format(job_name, str(int(time.time())))
        google_cloud_options.staging_location = gcs_staging_location
        google_cloud_options.temp_location = gcs_temp_location
        google_cloud_options.region = region
        options.view_as(StandardOptions).runner = 'DataflowRunner'

        setup_options = options.view_as(SetupOptions)
        setup_options.extra_packages = [whl_location]
    

    tfdv.generate_statistics_from_csv(
        data_location=input_data, 
        output_path=output_path,
        pipeline_options=options)

    return (output_path, )



import kfp

kfp.components.create_component_from_func(
    tfdv_generate_statistics,
    output_component_file='tfdv_generate_statistics_component.yaml', 
    base_image='gcr.io/pacific-torus-347809/mle-fp/preprocessing:latest')

print("done")

