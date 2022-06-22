import argparse
import logging
import logging.config
import os

from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError
from kfp.v2.dsl import component


def setup_credentials() -> None:

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pacific-torus.json"

def create_bucket(
    bucket_name: str,
    storage_class: str = "STANDARD",
    project_id: str = "pacific-torus-347809",
    location: str = "ASIA-SOUTHEAST1",
) -> None:
    """
    Create a new bucket in the ASIA region with the STANDARD storage
    class
    """

    storage_client = storage.Client(project=project_id)

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = storage_class

    new_bucket = storage_client.create_bucket(
        bucket_or_name=bucket, location=location, project=project_id
    )

    print(new_bucket.name)
    print(new_bucket.location)
    
    # logger.info(f"Created bucket: {new_bucket.name}")
    # logger.info(f"Location: {new_bucket.location}")
    # logger.info(f"Storage class: {new_bucket.storage_class}")


def bq_extract_data(
    source_project_id: str = None,
    source_dataset_id: str = None,
    source_table_id: str = None,
    destination_project_id: str = None,
    destination_bucket: str = None,
    destination_file: str = None,
    dataset_location: str = None,
    extract_job_config: dict = None,
) -> None:

    setup_credentials()

    # logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    storage_client = storage.Client(project=destination_project_id)

    if not storage.Bucket(storage_client, destination_bucket).exists():
        create_bucket(destination_bucket)
        logger.info(f"Bucket created {destination_bucket}")

    full_table_id = f"{source_project_id}.{source_dataset_id}.{source_table_id}"
    table = bigquery.table.Table(table_ref=full_table_id)

    if extract_job_config == "None" or extract_job_config is None:
        extract_job_config = {}
    if destination_file.endswith(".json"):
        extract_job_config = {"destination_format": "NEWLINE_DELIMITED_JSON"}
    job_config = bigquery.ExtractJobConfig(**extract_job_config)

    dataset_uri = f"gs://{destination_bucket}/{destination_file}"

    bq_client = bigquery.Client(project=destination_project_id)

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


def main(): 
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source_project_id', type=str, required=True)
    parser.add_argument(
        '--source_dataset_id', type=str, required=True)
    parser.add_argument(
        '--source_table_id', type=str, required=True)
    parser.add_argument(
        '--destination_project_id', type=str, required=True)
    parser.add_argument(
        '--destination_bucket', type=str, required=True)
    parser.add_argument(
        '--destination_file', type=str, required=True)
    parser.add_argument(
        '--dataset_location', type=str, required=True) 
    parser.add_argument(
        '--extract_job_config', type=str, required=True)
    args = parser.parse_args()


    bq_extract_data(
        source_project_id=args.source_project_id,
        source_dataset_id=args.source_dataset_id,
        source_table_id=args.source_table_id,
        destination_project_id=args.destination_project_id,
        destination_bucket=args.destination_bucket,
        destination_file=args.destination_file,
        dataset_location=args.dataset_location,
        extract_job_config=args.extract_job_config
    )

if __name__ == "__main__": 
    main()
