import os
from typing import NamedTuple

import google.cloud.aiplatform as aip
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import Artifact, Dataset, Input, Output, OutputPath, component

import kfp

PROJECT_ID = "pacific-torus-347809"
ML_PIPELINE_NAME = "ccd-train-pipeline" 
BUCKET_NAME = "mle-dwh-torus"
PIPELINE_ROOT = f"gs://{BUCKET_NAME}/pipeline/"
REGION = "us-central1"
URL_ROOT = "https://raw.githubusercontent.com/csanry/torus/main/pipelines/kfp_components"

aip.init(project=PROJECT_ID, staging_bucket=BUCKET_NAME)

# ----------------------------
# Load in necessary components 
# ----------------------------
# ingest components
ingest_op = kfp.components.load_component_from_url(f"{URL_ROOT}/ingest/bq_extract_data_component.yaml")

# preprocessing components 
basic_preprocessing_op = kfp.components.load_component_from_url(f"{URL_ROOT}/preprocessing/basic_preprocessing_component.yaml")
tfdv_op = kfp.components.load_component_from_url(f"{URL_ROOT}/preprocessing/tfdv_generate_statistics_component.yaml")
tfdv_drift_op = kfp.components.load_component_from_url(f"{URL_ROOT}/preprocessing/tfdv_detect_drift_component.yaml")

# training components 
train_test_split_data_op = kfp.components.load_component_from_url(f"{URL_ROOT}/training/train_test_split_data_component.yaml")
train_tune_op = kfp.components.load_component_from_url(f"{URL_ROOT}/training/train_hptune.yaml")
model_evaluation_op = kfp.components.load_component_from_url(f"{URL_ROOT}/training/model_evaluation.yaml")

# deploy components 
deploy_op = kfp.components.load_component_from_url(f"{URL_ROOT}/prediction/predict.yaml")


# -------------------
# Pipeline definition
# -------------------

@dsl.pipeline(
   name=ML_PIPELINE_NAME, 
   description='Train pipeline for credit card defaults', 
   pipeline_root=PIPELINE_ROOT
)
def ccd_train_pipeline(
):
    ingest = ingest_op(
        source_project_id=PROJECT_ID,
        source_table_url="dwh_pacific_torus.credit_card_defaults",
        destination_project_id=PROJECT_ID,
        destination_bucket=BUCKET_NAME,
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
        output_path=f"gs://{BUCKET_NAME}/tfdv_expers/eval/evaltest.pb",
        job_name='test-1',
        use_dataflow="False",
        project_id=PROJECT_ID,
        region=REGION, # asia-southeast1
        gcs_temp_location=f"gs://{BUCKET_NAME}/tfdv_expers/tmp",
        gcs_staging_location=f"gs://{BUCKET_NAME}/tfdv_expers",
        whl_location="tensorflow_data_validation-0.26.0-cp37-cp37m-manylinux2010_x86_64.whl"
    ).after(basic_preprocessing)

    # compare generated training data stats with stats from a previous version
    # of the training data set.
 
    tfdv_drift = tfdv_drift_op(
        stats_older_path="none", # "gs://mle-dwh-torus/stats/evaltest.pb", 
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
        ).after(train_test_split_data)

        model_eval = model_evaluation_op(
            trained_model = train_tune.outputs['model_path'],
            train_auc = train_tune.outputs['train_auc'],
            test_set = train_test_split_data.outputs['test_data'],
            threshold = 0.6
        ).after(train_tune)

        with dsl.Condition(model_eval.outputs['deploy'] == "True"):
            deploy = deploy_op(
                # model_input_file = model_eval.outputs['evaluated_model'],
                model_input_file=f"gs://{BUCKET_NAME}/models/",
                serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
                project_id=PROJECT_ID,
                region='REGION'
            )
            
    with dsl.Condition(tfdv_drift.outputs["drift"]=="false"):
        deploy = deploy_op(
            # model_input_file = model_eval.outputs['evaluated_model'],
            model_input_file=f"gs://{BUCKET_NAME}/models/",
            serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
            project_id=PROJECT_ID,
            region=REGION
            )

if __name__ == "__main__": 

    import subprocess
    from datetime import datetime 

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../pacific-torus.json"
    ID = datetime.now().strftime(f"%Y%m%d-%H%M")


    # ------------------------------------
    # Compile the function into a pipeline 
    # ------------------------------------

    compiler.Compiler().compile(
        pipeline_func=ccd_train_pipeline,
        pipeline_name=f"{ML_PIPELINE_NAME}-{ID}",
        package_path=f"{ML_PIPELINE_NAME}.json",
        type_check=True,
    )

    # ----------------------------------
    # Schedule and run a pipeline job
    # ----------------------------------
    
    job = aip.PipelineJob(
        display_name=f"{ML_PIPELINE_NAME}-{ID}",
        template_path=f"{ML_PIPELINE_NAME}.json",
        enable_caching=True,
        job_id=f"{ML_PIPELINE_NAME}-job-{ID}",
        pipeline_root=PIPELINE_ROOT
    )

    job.run()

    # remove files
    subprocess.call(["rm", "-r", f"{ML_PIPELINE_NAME}.json"])
