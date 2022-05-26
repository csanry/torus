import logging
import logging.config
import os

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from kfp.v2.dsl import component
from pipelines.kfp.dependencies import LOGGING_CONF, PROJECT_ID
from pipelines.kfp.helpers import create_bucket, setup_credentials


def bq_query_to_table(
    query: str,
    bq_client_project_id: str = None,
    destination_project_id: str = PROJECT_ID,
    dataset_id: str = None,
    table_id: str = None,
    dataset_location: str = "US",
    query_job_config: dict = None,
) -> None:

    setup_credentials()

    logging.config.fileConfig(LOGGING_CONF)
    logger = logging.getLogger("root")

    if (dataset_id is not None) and (table_id is not None):
        dest_table_ref = f"{destination_project_id}.{dataset_id}.{table_id}"
    else:
        dest_table_ref = None
    if query_job_config is None:
        query_job_config = {}
    job_config = bigquery.QueryJobConfig(destination=dest_table_ref, **query_job_config)

    logger.info("Set up client")

    bq_client = bigquery.Client(project=bq_client_project_id, location=dataset_location)

    logger.info("Running query")
    bq_client.query(query, job_config=job_config)

    logging.info(f"BQ table {dest_table_ref} created")


# test
# sql_query = """
# SELECT DISTINCT
#   id,
#   limit_balance,
#   sex,
#   education_level,
#   marital_status,
#   age,
#   pay_0,
#   pay_2,
#   pay_3,
#   pay_4,
#   pay_5,
#   pay_6,
#   bill_amt_1,
#   bill_amt_2,
#   bill_amt_3,
#   bill_amt_4,
#   bill_amt_5,
#   bill_amt_6,
#   pay_amt_1,
#   pay_amt_2,
#   pay_amt_3,
#   pay_amt_4,
#   pay_amt_5,
#   pay_amt_6,
#   default_payment_next_month,
# FROM `bigquery-public-data.ml_datasets.credit_card_default`
# """

# test
# bq_query_to_table(
#     query=sql_query,
#     bq_client_project_id=None,
#     destination_project_id=PROJECT_ID,
#     dataset_id="dwh_pacific_torus",
#     table_id="credit_card_default",
#     dataset_location="US",
#     query_job_config=None,
# )
