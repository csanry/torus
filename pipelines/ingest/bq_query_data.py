import logging
import logging.config
import os

from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError
from pipelines.kfp.dependencies import LOGGING_CONF, PROJECT_ID
from pipelines.kfp.helpers import create_bucket, setup_credentials


def bq_query_to_table(
    query: str,
    source_project_id: str = None,
    source_dataset_id: str = None,
    source_table_id: str = None,
    dest_project_id: str = PROJECT_ID,
    dest_bucket: str = None,
    dest_file: str = None,
    dataset_location: str = None,
    query_job_config: dict = None,
) -> None:

    setup_credentials()

    logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    storage_client = storage.Client(project=dest_project_id)

    if not storage.Bucket(storage_client, dest_bucket).exists():
        create_bucket(dest_bucket)
        logger.info(f"Bucket created {dest_bucket}")

    full_table_id = f"{source_project_id}.{source_dataset_id}.{source_table_id}"

    dataset_uri = f"gs://{dest_bucket}/{dest_file}"

    if query_job_config is None:
        query_job_config = {}
    job_config = bigquery.QueryJobConfig(**query_job_config)

    bq_client = bigquery.Client()

    logger.info(f"Query {full_table_id} and extract to {dataset_uri}")

    query_job = bq_client.query(
        query=query, job_config=job_config, location=dataset_location,
    ).to_dataframe()

    try:
        dataset_directory = os.path.dirname(dataset_uri)

        storage_client = storage.Client(project=dest_project_id)
        bucket = storage_client.get_bucket(bucket_or_name=dest_bucket)

        blob = bucket.blob(dest_file)
        blob.upload_from_string(data=query_job.to_parquet(), content_type="parquet")
        logger.info(f"Table extracted: {full_table_id}")

    except GoogleCloudError as e:
        logger.error(e)
        raise e

    return dataset_directory, dataset_uri

