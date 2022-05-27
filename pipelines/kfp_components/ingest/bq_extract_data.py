from kfp.v2.dsl import component
from pipelines.kfp_components.dependencies import (GOOGLE_CLOUD_BIGQUERY,
                                                   LOGGING_CONF, PROJECT_ID,
                                                   PYTHON37)


@component(base_image=PYTHON37, packages_to_install=[GOOGLE_CLOUD_BIGQUERY])
def bq_extract_data(
    source_project_id: str = None,
    source_dataset_id: str = None,
    source_table_id: str = None,
    destination_project_id: str = PROJECT_ID,
    destination_bucket: str = None,
    destination_file: str = None,
    dataset_location: str = None,
    extract_job_config: dict = None,
) -> None:

    import logging
    import logging.config
    import os
    import subprocess

    from google.cloud import bigquery, storage
    from google.cloud.exceptions import GoogleCloudError
    from pipelines.kfp_components.helpers import (create_bucket,
                                                  setup_credentials)

    setup_credentials()

    logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    storage_client = storage.Client(project=destination_project_id)

    if not storage.Bucket(storage_client, destination_bucket).exists():
        create_bucket(destination_bucket)
        logger.info(f"Bucket created {destination_bucket}")

    full_table_id = f"{source_project_id}.{source_dataset_id}.{source_table_id}"
    table = bigquery.table.Table(table_ref=full_table_id)

    if extract_job_config is None:
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


# test
# dd, duri = bq_extract_data(
#     source_project_id="pacific-torus-347809",
#     source_dataset_id="dwh_pacific_torus",
#     source_table_id="credit_card_default",
#     destination_project_id="pacific-torus-347809",
#     destination_bucket="mle-dwh-torus",
#     destination_file="raw/credit_cards.csv",
#     dataset_location="US",
# )

# print(dd, duri)
