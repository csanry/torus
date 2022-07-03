from importlib.metadata import metadata
from typing import NamedTuple

from kfp.v2.dsl import component, Output, Model

@component(output_component_file="train_hptune.yaml",
        base_image="gcr.io/pacific-torus-347809/mle-fp/base:latest",
        packages_to_install=["xgboost", "gcsfs", "sklearn"])
def train_hptune(
        train_file: str,
        trained_model: Output[Model],
) -> NamedTuple("outputs", [("model_path", str), ('train_auc', float)]):

    from sklearn.model_selection import RandomizedSearchCV
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import roc_auc_score
    from datetime import datetime
    from google.cloud import storage
    import pandas as pd
    import os
    import pickle
    
    train_df = pd.read_csv(train_file)

    PARAMS = {
        'n_estimators': [200, 300, 400],
    }

    PARAM_COMB = 2

    random_search = RandomizedSearchCV(RandomForestClassifier(),
                                       param_distributions=PARAMS,
                                       n_iter=PARAM_COMB,
                                       scoring='accuracy',
                                       verbose=2,
                                       random_state=2022
                                       )

    random_search.fit(train_df.drop(columns=["target"]), train_df.target)

    best_params = random_search.best_params_

    rf_model = RandomForestClassifier(**best_params)

    X = train_df.drop(['target'], axis = 1)
    Y = train_df['target']
    rf_model.fit(X, Y)

    y_prob = rf_model.predict_proba(X)[:, 1]
    train_auc = roc_auc_score(Y, y_prob)

    trained_model.metadata['framework'] = 'Random_Forest'
    trained_model.metadata['parameters'] = str(best_params)
    trained_model.metadata['train_auc'] = train_auc

    model_output_path = "gs://mle-dwh-torus/models/"
    model_id = datetime.now().strftime(f"%d%H%M")
    model_filename = f'model_{model_id}.pkl'
    local_path = model_filename

    with open(local_path, 'wb') as model_file:
        pickle.dump(rf_model, model_file)
    
    storage_path = os.path.join(model_output_path, model_filename)
    blob = storage.blob.Blob.from_string(storage_path, client = storage.Client())
    blob.upload_from_filename(local_path)

    from collections import namedtuple
    results = namedtuple("outputs", ["model_path", "train_auc"])
    return results(storage_path, train_auc)