from typing import NamedTuple
from kfp.v2.dsl import component, Dataset, Output

@component(output_component_file='train_test_split_data_component.yaml', 
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
        packages_to_install=["fsspec", "gcsfs", "sklearn"])
def train_test_split(
    input_file: str,
    output_bucket: str,
    train_set: Output[Dataset],
    test_set: Output[Dataset]
) -> NamedTuple("outputs", [("train_data", str), ("test_data", str)]):

    import pandas as pd
    from sklearn.model_selection import train_test_split

    df = pd.read_csv(input_file)
    train, test = train_test_split(df, test_size=0.2, random_state=2022)

    train_set.metadata['observations'] = train.shape[0]
    train_set.metadata['no._features'] = train.shape[1]
    train_set.metadata['column_names'] = list(train.columns)
    test_set.metadata['observations'] = test.shape[0]
    test_set.metadata['no._features'] = train.shape[1]
    test_set.metadata['column_names'] = list(test.columns)

    output_train_path = f"gs://mle-dwh-torus/{output_bucket}/train.csv"
    output_test_path = f"gs://mle-dwh-torus/{output_bucket}/test.csv" 

    train.to_csv(output_train_path, index = False)
    test.to_csv(output_test_path, index = False)

    from collections import namedtuple

    results = namedtuple("outputs", ["train_data", "test_data"])
    
    return results(output_train_path, output_test_path)