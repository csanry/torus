from typing import NamedTuple

from kfp.components import InputPath
from kfp.v2.dsl import Artifact


def train_test_split(
    input_file: InputPath("CSV"),
    output_bucket: str,
) -> NamedTuple("outputs", [
    ("train_data", Artifact),
    ("test_data", Artifact)
]):

    import pandas as pd
    from sklearn.model_selection import train_test_split

    df = pd.read_csv(input_file)
    train, test = train_test_split(df, test_size=0.2, random_state=2022)

    output_train_path = f"gs://mle-dwh-torus/{output_bucket}/train.csv"
    output_test_path = f"gs://mle-dwh-torus/{output_bucket}/test.csv" 

    train.to_csv(output_train_path)
    test.to_csv(output_test_path)

    from collections import namedtuple

    results = namedtuple("outputs", ["train_data", "test_data"])
    
    return results(train, test)


if __name__ == "__main__": 

    import kfp

    kfp.components.create_component_from_func(
        train_test_split,
        output_component_file='train_test_split_data_component.yaml', 
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
        packages_to_install=["fsspec", "gcsfs", "sklearn"])
        # extra_code="from kfp.v2.dsl import Dataset, InputPath",
