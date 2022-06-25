from typing import NamedTuple

from kfp.components import InputPath, OutputPath
from kfp.v2.dsl import Artifact, Model


def train_hptune(
        train_file: str,
        #model_bucket: str,
) -> NamedTuple("outputs", [
    ("final_model", str)
]):

    from xgboost import XGBClassifier
    from google.cloud import storage
    from sklearn.model_selection import RandomizedSearchCV
    import pandas as pd
    import os
    import pickle
    
    train_df = pd.read_csv(train_file)


    PARAMS = {
        'n_estimators': [200, 300, 400],
        'learning_rate': [0.01, 0.1, 1]
    }

    XGB = XGBClassifier()
    PARAM_COMB = 3

    random_search = RandomizedSearchCV(XGB,
                                       param_distributions=PARAMS,
                                       n_iter=PARAM_COMB,
                                       scoring='accuracy',
                                       verbose=3,
                                       random_state=2022
                                       )

    random_search.fit(train_df.drop(columns=["target"]), train_df.target)
    best_params = random_search.best_params_
    # best_score = random_search.best_score_

    xg_model = XGBClassifier(**best_params)
    xg_model.fit(train_df.drop(columns=["target"]), train_df.target)
    # score = xg_model.score(train_df.drop(columns=["target"]), train_df.target)

    artifact_filename = 'model.pkl'

    local_path = artifact_filename
    with open(local_path, 'wb') as model_file:
        pickle.dump(xg_model, model_file)

    model_directory = "gs://mle-dwh-torus/models/deployed/"
    storage_path = os.path.join(model_directory, artifact_filename)
    blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
    blob.upload_from_filename(local_path)

    # model_output_path = "gs://mle-dwh-torus/models/deployed/model.pkl"
    # # final_model = xg_model.save_model(model_output_path)
    # #
    # # file_name = final_model.path + f".pkl"
    # with open(model_output_path, 'wb') as file:
    #     pickle.dump(xg_model, file)
    #
    # final_model.metadata["framework"] = "XGBoost"
    # final_model.metadata["train_score"] = float(score)

    from collections import namedtuple

    results = namedtuple("outputs", ["final_model", ])

    return results(storage_path,)


if __name__ == "__main__":
    import kfp

    kfp.components.func_to_container_op(
        train_hptune,
        # extra_code="from kfp.v2.dsl import Artifact, Dataset, InputPath, OutputPath",
        output_component_file="train_hptune.yaml",
        base_image="gcr.io/pacific-torus-347809/mle-fp/base:latest",
        packages_to_install=["xgboost", "gcsfs", "sklearn"]
    )
