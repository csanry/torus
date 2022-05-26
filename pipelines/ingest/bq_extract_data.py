import logging
import logging.config
import os
import subprocess

from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError
from pipelines.kfp.dependencies import LOGGING_CONF, PROJECT_ID
from pipelines.kfp.helpers import create_bucket, setup_credentials


def bq_extract_data(
    source_project_id: str = None,
    source_dataset_id: str = None,
    source_table_id: str = None,
    dest_project_id: str = PROJECT_ID,
    dest_bucket: str = None,
    dest_file: str = None,
    dataset_location: str = None,
    extract_job_config: dict = None,
) -> None:

    setup_credentials()

    logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    storage_client = storage.Client(project=dest_project_id)

    if not storage.Bucket(storage_client, dest_bucket).exists():
        create_bucket(dest_bucket)
        logger.info(f"Bucket created {dest_bucket}")

    full_table_id = f"{source_project_id}.{source_dataset_id}.{source_table_id}"
    table = bigquery.table.Table(table_ref=full_table_id)

    if extract_job_config is None:
        extract_job_config = {}
    if dest_file.endswith(".json"):
        extract_job_config = {"destination_format": "NEWLINE_DELIMITED_JSON"}
    job_config = bigquery.ExtractJobConfig(**extract_job_config)

    dataset_uri = f"gs://{dest_bucket}/{dest_file}"

    bq_client = bigquery.Client(project=dest_project_id)

    logger.info(f"Extract {full_table_id} to {dataset_uri}")
    extract_job = bq_client.extract_table(
        source=table,
        destination_uris=dataset_uri,
        job_config=job_config,
        location=dataset_location,
    )

    dataset_directory = os.path.dirname(dataset_uri)

    try:
        extract_job.result()
        logger.info(f"Table extracted: {full_table_id}")
    except GoogleCloudError as e:
        logger.error(e)
        logger.error(extract_job.error_result)
        logger.error(extract_job.errors)
        raise e

    return dataset_directory, dataset_uri

