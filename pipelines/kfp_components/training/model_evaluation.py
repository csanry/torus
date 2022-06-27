from typing import NamedTuple
from kfp.components import InputPath, OutputPath
from kfp.v2.dsl import Artifact, Model


def model_evaluation(
        trained_model: str,
        train_auc: float,
        test_set: str,
        threshold: float,
        deploy: str = 'False',
) -> NamedTuple("Outputs", [('deploy', str), ('evaluated_model', str)]):
    from xgboost import XGBClassifier
    from google.cloud import storage
    import os
    import pandas as pd
    import pickle
    import json
    from sklearn.metrics import roc_auc_score, confusion_matrix

    # Load Datasets and Models
    data = pd.read_csv(test_set)
    model = XGBClassifier()

    model_bucket = trained_model.split("/")[2]
    object_name = "/".join(trained_model.split("/")[3:])

    client = storage.Client()
    bu = client.bucket(model_bucket)
    b = bu.blob(object_name)
    b.download_to_filename('model.pkl')
    model = pickle.load(open('model.pkl', 'rb'))

    X = data.drop(['target'], axis=1)
    Y = data['target']

    y_prob = model.predict_proba(X)[:, 1]
    y_pred = model.predict(X)
    auc = roc_auc_score(Y, y_prob)

    model_info = {
        'framework': 'RF',
        'train_auc': train_auc,
        'test_auc': auc,
        'confusion_matrix': {'classes': [0, 1], 'matrix': confusion_matrix(Y, y_pred).tolist()}
    }

    # Make Checks Before Deployment 
    # Check if a deployed model exists with associated information and make performance comparisons

    model_loc = "gs://mle-dwh-torus/models/deployed/model_info.json"
    info_bucket = model_loc.split("/")[2]
    file_name = "/".join(model_loc.split("/")[3:])
    bucket = client.bucket(info_bucket)
    info_exists = storage.Blob(bucket=bucket, name=file_name).exists(client)
    print("info_exists", info_exists)
    print("AUC", auc)
    model_output_path = 'gs://mle-dwh-torus/models/deployed/'
    model_filename = 'model_info.json'
    storage_path = os.path.join(model_output_path, model_filename)

    from collections import namedtuple
    results = namedtuple("outputs", ['deploy', 'evaluated_model'])

    if not info_exists and auc > threshold:
        deploy = 'True'
        blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
        blob.upload_from_string(data=json.dumps(model_info), content_type='application/json')
        return results(deploy, trained_model)

    elif info_exists:
        blob = bucket.get_blob(file_name)
        info = json.loads(blob.download_as_string())

        if auc >= info['test_auc']:
            deploy = 'True'
            blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
            blob.upload_from_string(data=json.dumps(model_info), content_type='application/json')
            return results(deploy, trained_model)

        else:
            return results(deploy, trained_model)


if __name__ == "__main__":
    import kfp

    kfp.components.create_component_from_func(
        model_evaluation,
        output_component_file='model_evaluation.yaml',
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
        packages_to_install=["pandas", "gcsfs", "fsspec", "sklearn", "xgboost"])
