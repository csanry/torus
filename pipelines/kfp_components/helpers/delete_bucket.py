import logging
import logging.config

from google.cloud import storage
from pipelines.kfp_components.dependencies import LOGGING_CONF, PROJECT_ID
from pipelines.kfp_components.helpers import setup_credentials


def delete_bucket(
    bucket_name: str, project_id: str = PROJECT_ID,
):
    """Deletes a bucket. The bucket must be empty."""

    logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    setup_credentials()
    storage_client = storage.Client(project=project_id)

    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete(force=True)

    logger.info(f"Bucket {bucket.name} deleted")
