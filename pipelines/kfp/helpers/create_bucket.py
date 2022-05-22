import logging
import logging.config
import os

from google.cloud import storage
from pipelines.kfp.dependencies import BUCKET_LOCATION, LOGGING_CONF, PROJECT_ID
from pipelines.kfp.helpers.setup_credentials import setup_credentials


def create_bucket(
    bucket_name: str,
    storage_class: str = "STANDARD",
    project_id: str = PROJECT_ID,
    location: str = BUCKET_LOCATION,
) -> None:
    """
    Create a new bucket in the ASIA region with the STANDARD storage
    class
    """

    setup_credentials()

    logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    storage_client = storage.Client(project=project_id)

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = storage_class

    new_bucket = storage_client.create_bucket(
        bucket_or_name=bucket, location=location, project=project_id
    )

    logger.info(f"Created bucket: {new_bucket.name}")
    logger.info(f"Location: {new_bucket.location}")
    logger.info(f"Storage class: {new_bucket.storage_class}")
