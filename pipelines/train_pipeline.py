import os
from typing import NamedTuple

import google.cloud.aiplatform as aip
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import Artifact, Dataset, Input, Output, OutputPath, component
from sqlalchemy import outparam

import kfp

PIPELINE_ROOT = "{}/pipeline/".format("gs://mle-dwh-torus")

aip.init(project="pacific-torus-347809", staging_bucket="gs://mle-dwh-torus")



ingest_op = kfp.components.load_component_from_file("./kfp_components/ingest/ingest_component.yaml")
tfdv_op = kfp.components.load_component_from_file("./kfp_components/preprocessing/tfdv_generate_statistics_component.yaml")


tfdv_drift_op = kfp.components.load_component_from_file("./kfp_components/preprocessing/tfdv_detect_drift_component.yaml")


basic_preprocessing_op = kfp.components.load_component_from_file("./kfp_components/preprocessing/basic_preprocessing_component.yaml")
train_test_split_data_op = kfp.components.load_component_from_file("./kfp_components/training/train_test_split_data_component.yaml")



# Define the pipeline
@dsl.pipeline(
   name='testing pipeline', # to change 
   description='NIL', # to change 
   pipeline_root=PIPELINE_ROOT
)
def xgboost_test_pipeline(
):
    ingest = ingest_op(
        source_project_id="pacific-torus-347809",
        source_table_url="dwh_pacific_torus.credit_card_defaults",
        destination_project_id="pacific-torus-347809",
        destination_bucket="mle-dwh-torus",
        destination_file="raw/new_ccd_test_02.csv",
        dataset_location="US",
        extract_job_config={},
    ) # .apply(gcp.use_gcp_secret('user-gcp-sa'))

    basic_preprocessing = basic_preprocessing_op(
        input_file=ingest.outputs["dataset_gcs_uri"],
        output_bucket="int",
        output_file="ccd2_int.csv"
    )

    tfdv_step = tfdv_op(
        input_data=basic_preprocessing.output, # basic_preprocessing.output,
        output_path="gs://mle-dwh-torus/tfdv_expers/eval/evaltest.pb",
        job_name='test-1',
        use_dataflow="False",
        project_id="pacific-torus-347809",
        region="asia-southeast1-c", # us-central1
        gcs_temp_location='gs://mle-dwh-torus/tfdv_expers/tmp',
        gcs_staging_location='gs://mle-dwh-torus/tfdv_expers',
        whl_location="tensorflow_data_validation-0.26.0-cp37-cp37m-manylinux2010_x86_64.whl"
    ).after(basic_preprocessing)

    # compare generated training data stats with stats from a previous version
    # of the training data set.
    tfdv_drift = tfdv_drift_op(
        stats_older_path="gs://mle-dwh-torus/stats/evaltest.pb", 
        stats_new_path=tfdv_step.outputs['stats_path'],
        target_feature="limit_bal"
    )

    # if true with drift detected, run the train test split
    with dsl.Condition(tfdv_drift.outputs["drift"] == "true"):
        train_test_split_data = train_test_split_data_op(
            input_file=basic_preprocessing.output,
            output_bucket="fin"
        )




if __name__ == "__main__": 
    from datetime import datetime

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../pacific-torus.json"
    id = datetime.now().strftime(f"%d%H%M")

    compiler.Compiler().compile(
        pipeline_func=xgboost_test_pipeline,
        pipeline_name="xgboost-train-pipeline",
        package_path="./test.json",
        type_check=True,
    )

    job = aip.PipelineJob(
        display_name="testpipeline",
        template_path="./test.json",
        job_id=f'test-{id}',
        pipeline_root=PIPELINE_ROOT
    )

    job.run()
