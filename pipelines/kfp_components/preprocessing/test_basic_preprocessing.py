


from pipelines.kfp_components.preprocessing.basic_preprocessing import \
    basic_preprocessing


def test_basic_preprocessing(): 


    import pandas as pd
    from google.cloud import storage


    test_df = pd.DataFrame({
        "ID": [3, 4, 5, 6], 
        "LIMIT_BAL": [90_000, 50_000, 50_000, 500_000],
        "SEX": ["F", "F", "M", "M"],
        "EDUCATION": ["University", "0", "High School", "0"],
        "MARRIAGE": ["Single", "0", "Married", "0"],
        "default_": ["Y", "N", "N", "Y"] 
        })

    test_df.to_csv("gs://mle-dwh-torus/raw/test_dataset.csv", index=False)
    

    # test_basic_preprocessing

    basic_preprocessing(
        input_file="gs://mle-dwh-torus/raw/test_dataset.csv",
        output_bucket="int",
        output_file="test_dataset.csv"
    )


    # test whether path exists 
    storage_client = storage.Client(project="pacific-torus-347809")
    bucket = storage_client.bucket(bucket_name="mle-dwh-torus")

    test_path = "int/test_dataset.csv"
    blob = storage.Blob(name=test_path, bucket=bucket)

    check_exists = blob.exists(storage_client)

    assert check_exists 


    # check if conversion has been made for education

    df = pd.read_csv(f"gs://mle-dwh-torus/{test_path}")

    assert df.loc[1, "ed_Unknown"] == 1 
    assert df.loc[3, "ed_Unknown"] == 1


    # check if conversion has been made for marriage
    assert df.loc[1, "mstatus_Other"] == 1
    assert df.loc[3, "mstatus_Other"] == 1


    # check if figures have been transferred accurately
    assert df["limit_bal"].mean() == test_df["LIMIT_BAL"].mean()

    # clear items
    blob = storage.Blob(name="raw/test_dataset.csv", bucket=bucket)
    blob.delete()

    blob = storage.Blob(name="int/test_dataset.csv", bucket=bucket)
    blob.delete()


