from typing import NamedTuple
from kfp.v2.dsl import component, Output, ClassificationMetrics

@component(base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
            packages_to_install=["pandas", "gcsfs", "fsspec", "sklearn", "xgboost"],
            output_component_file='model_evaluation.yaml')
def model_evaluation(
        trained_model: str,
        train_auc: float,
        test_set: str,
        model_test: Output[ClassificationMetrics],
        threshold: float,
        deploy: str = 'False',
) -> NamedTuple("outputs", [('deploy', str), ('evaluated_model', str)]):
    from google.cloud import storage
    import os
    import pandas as pd
    import pickle
    import json
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import roc_auc_score, confusion_matrix, roc_curve

    # Load Datasets and Models
    data = pd.read_csv(test_set)
    model = RandomForestClassifier()

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
    test_auc = roc_auc_score(Y, y_prob)
    fpr, tpr, thresholds = roc_curve(Y, y_prob, pos_label=1)

    # Add model performance metadata
    model_test.metadata['test_auc'] = test_auc
    model_test.log_roc_curve(fpr.tolist(), tpr.tolist(), thresholds.tolist())
    model_test.log_confusion_matrix(['No Default', 'Default'], confusion_matrix(Y, y_pred).tolist())
    
    # Also export in a json in the model path
    model_info = {
        'framework': 'RF',
        'train_auc': train_auc,
        'test_auc': test_auc,
        'confusion_matrix': {'classes': [0, 1], 'matrix': confusion_matrix(Y, y_pred).tolist()}
    }

    # Make Checks Before Deployment 
    # Check if a deployed model exists with associated information and make performance comparisons

    model_info_loc = "gs://mle-dwh-torus/models/deployed/model_info.json"
    info_bucket = model_info_loc.split("/")[2]
    file_name = "/".join(model_info_loc.split("/")[3:])
    bucket = client.bucket(info_bucket)
    info_exists = storage.Blob(bucket=bucket, name=file_name).exists(client)
    model_output_path = 'gs://mle-dwh-torus/models/deployed/'
    model_info_filename = 'model_info.json'
    model_filename = 'model.pkl'

    info_storage_path = os.path.join(model_output_path, model_info_filename)
    model_storage_path = os.path.join(model_output_path, model_filename)

    from collections import namedtuple
    results = namedtuple("outputs", ['deploy', 'evaluated_model'])

    def set_params_upload():
        with open('model.pkl', 'wb') as model_file:
            pickle.dump(model, model_file)

        info_blob = storage.blob.Blob.from_string(info_storage_path, client=storage.Client())
        model_blob = storage.blob.Blob.from_string(model_storage_path, client=storage.Client())
        info_blob.upload_from_string(data=json.dumps(model_info), content_type='application/json')
        model_blob.upload_from_filename('model.pkl')

    if not info_exists and test_auc > threshold:
        deploy = 'True'
        set_params_upload()
        model_loc = "gs://mle-dwh-torus/models/deployed/model.pkl"
        return results(deploy, model_loc)

    elif info_exists:
        blob = bucket.get_blob(file_name)
        info = json.loads(blob.download_as_string())

        if test_auc >= info['test_auc']:
            deploy = 'True'
            set_params_upload()
            model_loc = "gs://mle-dwh-torus/models/deployed/model.pkl"
            return results(deploy, model_loc)

        else:
            return results(deploy, "Challenger model failed")