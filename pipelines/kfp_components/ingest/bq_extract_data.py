import os
from typing import NamedTuple

from kfp.v2.dsl import component


def bq_extract_data(
    source_project_id: str,
    source_table_url: str,
    destination_project_id: str,
    destination_bucket: str, 
    destination_file: str,
    dataset_location: str,
    extract_job_config: dict = None,
) -> NamedTuple('outputs', [('dataset_gcs_uri', str), 
    ('dataset_gcs_directory', str)]
):

    import logging
    import os

    from google.cloud import bigquery, storage
    from google.cloud.exceptions import GoogleCloudError


    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pacific-torus.json"

    # logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    storage_client = storage.Client(project=destination_project_id)

    if not storage.Bucket(storage_client, destination_bucket).exists():
        bucket = storage_client.bucket(destination_bucket)
        bucket.storage_class = "STANDARD" 
        storage_client.create_bucket(
            bucket_or_name=bucket, location="ASIA-SOUTHEAST1", project=destination_project_id
        )
        logger.info(f"Bucket created {destination_bucket}")

    full_table_url = f"{source_project_id}.{source_table_url}"
    table = bigquery.table.Table(table_ref=full_table_url)

    if extract_job_config is None:
        extract_job_config = {}
    if destination_file.endswith(".json"):
        extract_job_config = {"destination_format": "NEWLINE_DELIMITED_JSON"}
    job_config = bigquery.ExtractJobConfig(**extract_job_config)


    dataset_gcs_uri = f"gs://{destination_bucket}/{destination_file}"
 
    bq_client = bigquery.Client(project=destination_project_id)

    logger.info(f"Extract {source_table_url} to {dataset_gcs_uri}")
    extract_job = bq_client.extract_table(
        source=table,
        destination_uris=dataset_gcs_uri,
        job_config=job_config,
        location=dataset_location,
    )

    dataset_gcs_directory = os.path.dirname(dataset_gcs_uri)

    try:
        extract_job.result()
        logger.info(f"Table extracted: {dataset_gcs_uri}")
    except GoogleCloudError as e:
        logger.error(e)
        logger.error(extract_job.error_result)
        logger.error(extract_job.errors)
        raise e

    return (dataset_gcs_uri, dataset_gcs_directory)


if __name__ == "__main__": 
    import kfp

    kfp.components.func_to_container_op(
        bq_extract_data,
        extra_code="from typing import NamedTuple",
        output_component_file='./ingest_component.yaml', 
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest'
    )
