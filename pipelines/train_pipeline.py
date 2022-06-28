import os
from typing import NamedTuple

import google.cloud.aiplatform as aip
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import Artifact, Dataset, Input, Output, OutputPath, component
# from sqlalchemy import outparam

import kfp

PIPELINE_ROOT = "{}/pipeline/".format("gs://mle-dwh-torus")

aip.init(project="pacific-torus-347809", staging_bucket="gs://mle-dwh-torus")

ingest_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/ingest/ingest_component.yaml")
tfdv_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/preprocessing/tfdv_generate_statistics_component.yaml")

tfdv_drift_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/preprocessing/tfdv_detect_drift_component.yaml")

basic_preprocessing_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/preprocessing/basic_preprocessing_component.yaml")
train_test_split_data_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/training/train_test_split_data_component.yaml")

train_tune_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/training/train_hptune.yaml")
model_evaluation_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/training/model_evaluation.yaml")
deploy_op = kfp.components.load_component_from_url("https://raw.githubusercontent.com/csanry/torus/build_updates_v2/pipelines/kfp_components/prediction/predict.yaml")


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

    with dsl.Condition(tfdv_drift.outputs['drift']=='true'):
        train_test_split_data = train_test_split_data_op(
                input_file=basic_preprocessing.output,
                output_bucket="fin"
            )

        train_tune = train_tune_op(
            train_file = train_test_split_data.outputs['train_data']
        )

        model_eval = model_evaluation_op(
            trained_model = train_tune.outputs['model_path'],
            train_auc = train_tune.outputs['train_auc'],
            test_set = train_test_split_data.outputs['test_data'],
            threshold = 0.6
        )

        with dsl.Condition(model_eval.outputs['deploy'] == "True"):
            deploy = deploy_op(
                # model_input_file = model_eval.outputs['evaluated_model'],
                model_input_file = 	'gs://mle-dwh-torus/models/',
                serving_container_image_uri = "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
                project_id= 'pacific-torus-347809',
                region = 'us-central1'
            )
            
    with dsl.Condition(tfdv_drift.outputs['drift']=='false'):
        deploy = deploy_op(
                    # model_input_file = model_eval.outputs['evaluated_model'],
                    model_input_file = 	'gs://mle-dwh-torus/models/',
                    serving_container_image_uri = "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
                    project_id= 'pacific-torus-347809',
                    region = 'us-central1'
                )

if __name__ == "__main__": 
    from datetime import datetime

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "https://raw.githubusercontent.com/csanry/torus/build_updates_v2/data/pacific-torus.json"
    id = datetime.now().strftime(f"%d%H%M")

    compiler.Compiler().compile(
        pipeline_func=xgboost_test_pipeline,
        pipeline_name="ccd-train-pipeline",
        package_path="./test.json",
        type_check=True,
    )

    job = aip.PipelineJob(
        display_name="testpipeline",
        template_path="./test.json",
        enable_caching=True,
        job_id=f'rf-test-{id}',
        pipeline_root=PIPELINE_ROOT
    )

    job.run()
