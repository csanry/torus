import os
from typing import NamedTuple

from isort import check_file
from kfp.v2.dsl import component
from pipelines.kfp_components.ingest.bq_extract_data import bq_extract_data


def test_bq_extract_data():
    # import logging
    # import os

    import numpy as np
    import pandas as pd
    from google.cloud import bigquery, storage

    # Create a test_csv
    test_values = np.arange(1, 10)
    test_df = pd.DataFrame(columns=["test_column"], data=test_values)
    
    # upload to bigquery 
    bq_client = bigquery.Client(project="pacific-torus-347809")
    job_config = bigquery.LoadJobConfig(
        autodetect=True
    )

    table_id = "pacific-torus-347809.dwh_pacific_torus.test_dataset" 

   
    job = bq_client.load_table_from_dataframe(
        dataframe=test_df, 
        destination=table_id,
        job_config=job_config
    )
    job.result()

    # run the function 
    bq_extract_data(
        source_project_id="pacific-torus-347809",
        source_table_url="dwh_pacific_torus.test_dataset",
        destination_project_id="pacific-torus-347809",
        destination_bucket="mle-dwh-torus",
        destination_file="raw/test_dataset.csv",
        dataset_location="US",
        extract_job_config={},
    ) 

    test_file_name = "raw/test_dataset.csv"

    # assert bucket exists
    storage_client = storage.Client(project="pacific-torus-347809")
    bucket = storage_client.bucket("mle-dwh-torus")

    assert bucket.exists()


    # assert file exists 
    blob = storage.Blob(bucket=bucket, name=test_file_name)
    
    check_file_exists = blob.exists(storage_client)

    assert check_file_exists

    # assert that column name is read in properly
    df = pd.read_csv("gs://mle-dwh-torus/raw/test_dataset.csv")
    assert df.columns == "test_column"

    # assert values are read in properly
    assert df["test_column"].tail().to_list() == [5, 6, 7, 8, 9]


    # clear env
    bq_client.delete_table(table_id, not_found_ok=True)
    blob.delete()
    
