import os
from datetime import datetime
from typing import NamedTuple

import google.cloud.aiplatform as aip
from kfp.v2 import compiler, dsl

import kfp

PROJECT_ID = "pacific-torus-347809"
ML_PIPELINE_NAME = "ccd-train-pipeline" 
BUCKET_NAME = "mle-dwh-torus"
PIPELINE_ROOT = f"gs://{BUCKET_NAME}/pipeline/"
REGION = "us-central1"
URL_ROOT = "https://raw.githubusercontent.com/csanry/torus/main/pipelines/kfp_components"
YEAR_MONTH = datetime.now().strftime(f"%Y-%m")
BASE_FILE_NAME = "credit-card-defaults"
aip.init(project=PROJECT_ID, staging_bucket=BUCKET_NAME)

# ----------------------------
# Load in necessary components 
# ----------------------------
# ingest components
ingest_op = kfp.components.load_component_from_url(f"{URL_ROOT}/ingest/bq_extract_data_component.yaml")

# preprocessing components 
basic_preprocessing_op = kfp.components.load_component_from_url(f"{URL_ROOT}/preprocessing/basic_preprocessing_component.yaml")
tfdv_generate_statistics_op = kfp.components.load_component_from_url(f"{URL_ROOT}/preprocessing/tfdv_generate_statistics_component.yaml")
tfdv_detect_drift_op = kfp.components.load_component_from_url(f"{URL_ROOT}/preprocessing/tfdv_detect_drift_component.yaml")
tfdv_visualise_statistics_op = kfp.components.load_component_from_url(f"{URL_ROOT}/preprocessing/tfdv_visualise_statistics_component.yaml")

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


    # ingestion step
    ingest_step = (
        ingest_op(
            source_project_id=PROJECT_ID,
            source_table_url="dwh_pacific_torus.credit_card_defaults",
            destination_project_id=PROJECT_ID,
            destination_bucket=BUCKET_NAME,
            destination_file=f"raw/{BASE_FILE_NAME}-{YEAR_MONTH}.csv",
            dataset_location="US",
            extract_job_config={},
        )
        .set_display_name("Extract data from BQ")
    ) 

    # preprocessing steps
    basic_preprocessing = (
        basic_preprocessing_op(
            input_file=ingest_step.outputs["dataset_gcs_uri"],
            output_bucket="int",
            output_file=f"{BASE_FILE_NAME}-cleaned-{YEAR_MONTH}.csv"
        )
        .after(ingest_step)
        .set_display_name("Basic preprocessing")
    )


    tfdv_generate_statistics_step = (
        tfdv_generate_statistics_op(
            input_data=basic_preprocessing.output, # basic_preprocessing.output,
            output_path=f"gs://{BUCKET_NAME}/tfdv_expers/eval/evaltest.pb",
            job_name='test-1',
            use_dataflow="False",
            project_id=PROJECT_ID,
            region=REGION, # asia-southeast1
            gcs_temp_location=f"gs://{BUCKET_NAME}/tfdv_expers/tmp",
            gcs_staging_location=f"gs://{BUCKET_NAME}/tfdv_expers",
            whl_location="tensorflow_data_validation-0.26.0-cp37-cp37m-manylinux2010_x86_64.whl"
        )
        .after(basic_preprocessing)
        .set_display_name("Generate stats")
    )


    # compare generated training data stats with stats from a previous version
    # of the training data set
    tfdv_detect_drift_step = (
        tfdv_detect_drift_op(
            stats_older_path="none", # "gs://mle-dwh-torus/stats/evaltest.pb", 
            stats_new_path=tfdv_generate_statistics_step.outputs['stats_path'],
            target_feature="limit_bal"
        )
        .after(tfdv_generate_statistics_step)
        .set_display_name("Drift detection")
    )

    visualise_statistics_step = (
        tfdv_visualise_statistics_op(
            statistics_path=tfdv_generate_statistics_step.outputs["stats_path"],  
            output_bucket=f"{BUCKET_NAME}/stats",
            statistics_name="Month of July"
        )
        .after(tfdv_generate_statistics_step)
        .set_display_name("Visualise statistics")
    )

    # if drift is detect, run training components 
    with dsl.Condition(tfdv_detect_drift_step.outputs['drift']=='true', name="drift-detected"):
        
        # train test split
        train_test_split_data_step = (
            train_test_split_data_op(
                input_file=basic_preprocessing.output,
                output_bucket="fin"
            )
            .set_display_name("Train test split")
        )

        # train a challenger model
        train_tune_step = (
            train_tune_op(
                train_file=train_test_split_data_step.outputs['train_data']
            )
            .after(train_test_split_data_step)
            .set_display_name("Hyperparameter tuning")
        )

        # compare against the champion model
        model_eval_step = (
            model_evaluation_op(
                trained_model = train_tune_step.outputs['model_path'],
                train_auc = train_tune_step.outputs['train_auc'],
                test_set = train_test_split_data_step.outputs['test_data'],
                threshold = 0.6
            )
            .after(train_tune_step)
            .set_display_name("Evaluation of model")
        )

        # if challenger is better, deploy the challenger
        with dsl.Condition(model_eval_step.outputs['deploy'] == "True", name="deploy-new-model"):
            deploy_challenger = (
                deploy_op(
                    # model_input_file = model_eval.outputs['evaluated_model'],
                    model_input_file=f"gs://{BUCKET_NAME}/models/",
                    serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
                    project_id=PROJECT_ID,
                    region='REGION'
                )
                .set_display_name("Deploy the new model")
            )
        
    with dsl.Condition(tfdv_detect_drift_step.outputs["drift"]=="false", name="no-drift-detected"):
        deploy_champion = (
            deploy_op(
            # model_input_file = model_eval.outputs['evaluated_model'],
            model_input_file=f"gs://{BUCKET_NAME}/models/",
            serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
            project_id=PROJECT_ID,
            region=REGION
            )
            .set_display_name("Serve current model")
        )


if __name__ == "__main__": 

    import subprocess

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../pacific-torus.json"
    ID = datetime.now().strftime(f"%Y-%m-%d-%H%M")


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
