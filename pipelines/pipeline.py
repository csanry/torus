import json
import pathlib

from google_cloud_pipeline_components import aiplatform as gcc_aip
from google_cloud_pipeline_components.experimental.custom_job.utils import \
    create_custom_training_job_op_from_component
from kfp.v2 import compiler, dsl

from kfp_components.dependencies import PROJECT_ID, ROOT_DIR
from pipelines.kfp_components.helpers import generate_query
# from pipelines.kfp.helpers import copy_artifact
# from pipelines.training import train_xgboost_model
from pipelines.kfp_components.ingest import bq_extract_data, bq_query_to_table


@dsl.pipeline(name="xgboost-train-pipeline")
def xgboost_pipeline(
    # project_id: str,
    # project_location: str,
    # pipeline_files_gcs_path: str,
    # ingestion_project_id: str,
    # tfdv_schema_filename: str,
    # tfdv_train_stats_path: str,
    # model_name: str,
    # model_label: str,
    # dataset_id: str,
    # dataset_location: str,
    # ingestion_dataset_id: str,
    # timestamp: str,
):
    """
    Query a view from BQ
    Extract the view to GCS

    """

    sql_query = generate_query(
        input_file=ROOT_DIR
        / "pipelines"
        / "kfp_components"
        / "ingest"
        / "queries"
        / "query_bq.sql"
    )

    ingest = bq_query_to_table(
        query=sql_query,
        destination_project_id=PROJECT_ID,
        dataset_id="dwh_pacific_torus",
        table_id="credit_card_default",
        dataset_location="US",
    ).set_display_name("Ingest data")


    ## Ingest to GCS

    ingest_to_gcs = (
        bq_extract_data(
            source_project_id="pacific-torus-347809",
            source_dataset_id="dwh_pacific_torus",
            source_table_id="credit_card_default",
            destination_project_id="pacific-torus-347809",
            destination_bucket="mle-dwh-torus",
            destination_file="raw/credit_cards.csv",
            dataset_location="US",
        )
        .after(ingest)
        # .set_display_name("Export to GCS")
    )

    # create dataset
    dataset = gcc_aip.TabularDatasetCreateOp(
        project="pacific-torus-347809",
        display_name="credit_cards_test_2705",
        bq_source="bq://dwh_pacific_torus.credit_card_default"
    ).after(ingest_to_gcs)


    

def compile():

    compiler.Compiler().compile(
        pipeline_func=xgboost_pipeline,
        pipeline_name="xgboost-train-pipeline",
        package_path="training.json",
        type_check=False,
    )


if __name__ == "__main__":

    # custom_train_job = create_custom_training_job_op_from_component(
    #     component_spec=train_xgboost_model,
    #     replica_count=1,
    #     machine_type="n1-standard-4",
    # )

    compile()
