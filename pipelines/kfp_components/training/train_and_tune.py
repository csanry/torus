from typing import NamedTuple
from kfp.v2.dsl import Artifact, InputPath


def train_and_tune(
    train_file: InputPath("CSV"),
    test_file: InputPath("CSV")
    model_bucket: str,
) -> NamedTuple("outputs", [("final_model", Artifact)]):


    from xgboost import XGBClassifier
    from sklearn.model_selection import RandomizedSearchCV
    import json
    import pandas as pd

    train_df = pd.read_csv(train_file)

    PARAMS = {
            'n_estimators'  : [200,300,400],
            'learning_rate' : [0.01,0.1,1]
            }

    XGB = XGBClassifier(learning_rate=0.2, n_estimators=600)
    PARAM_COMB = 3

    random_search = RandomizedSearchCV(xgb,
                                        param_distributions=PARAMS,
                                        n_iter=PARAM_COMB,
                                        scoring = 'roc_auc'
                                        n_jobs=-1,
                                        verbose=3,
                                        random_state=2022
                                        )


    best_params = random_search.best_params_
    best_score = random_search.best_score_

    xg_model = XGBClassifier(**best_params)
    xg_model.fit(train_df.drop(columns=["target"]), train_df.target)
    score = xg_model.score(train_df.drop(columns=["target"]), train_df.target)

    final_model.metadata["framework"] = "XGBoost"
    final_model.metadata["train_score"] = float(score)

    model_output_path = f"gs://mle-dwh-torus/{model_bucket}/model.bst"
    xg_model.save_model(model_output_path)

    with open("hparams.json", "w") as outfile:
        json.dump(best_params, outfile)

    return (xgmodel,)


if __name__ == "__main__":

    import kfp

    kfp.components.func_to_container_op(
        train_and_tune,
        extra_code="from kfp.v2.dsl import Artifact, Dataset, InputPath, OutputPath",
        output_component_file="train_and_tune.yaml",
        base_image="gcr.io/pacific-torus-347809/mle-fp/base:latest",
        packages_to_install=["xgboost", "gcsfs", "json", "sklearn"]
    )
