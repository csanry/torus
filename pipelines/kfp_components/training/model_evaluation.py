from typing import NamedTuple
from kfp.components import InputPath, OutputPath
from kfp.v2.dsl import Artifact, Model

def model_evaluation(
    deployed_model_bucket: str,
    trained_model: InputPath(Model),
    test_set: str,
    threshold: float,
    deploy: str = 'False',
) -> NamedTuple("Outputs", [('deploy', str), ('evaluated_model', Model)]):

    from xgboost import XGBClassifier
    from google.cloud import storage
    import pandas as pd
    from sklearn.metrics import roc_auc_score, confusion_matrix

    # Load Datasets and Models
    data = pd.read_csv(test_set)
    model = XGBClassifier()
    model.load_model(trained_model)

    X = data.drop(['target'], axis = 1)
    Y = data['target']
    
    y_prob = model.predict_proba(X)[:, 1]
    y_pred = model.predict(X)
    auc = roc_auc_score(Y, y_prob)

    trained_model.metadata['auc'] = auc
    trained_model.metadata['confusion_matrix'] = {'classes': [0, 1], 'matrix': confusion_matrix(Y, y_pred).tolist()}

    # Test versus Current Model and Threshold
    client = storage.Client()
    bucket = client.get_bucket(deployed_model_bucket)
    blobs = list(client.list_blobs(bucket))

    if len(blobs) == 0 and auc > threshold:
        deploy = 'True'
        return (deploy, trained_model)
    elif len(blobs) == 1: 
        if auc > blobs[0].metadata['auc']:
            deploy = 'True'
            return (deploy, trained_model)
    
    return (deploy, None)

if __name__ == "__main__": 

    import kfp

    kfp.components.create_component_from_func(
        model_evaluation,
        output_component_file='model_evaluation.yaml', 
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
        packages_to_install=["pandas", "sklearn", "xgboost"])