from typing import NamedTuple
from kfp.components import InputPath, OutputPath
from kfp.v2.dsl import Artifact

def model_evaluation(
    trained_model: InputPath(Artifact),
    test_set: InputPath('csv'),
    threshold: int,
    deploy = 'no',
) -> NamedTuple("Outputs", [('deploy', str)]):

    from xgboost import XGBClassifier
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

    if auc > threshold:
        deploy = 'yes'

    # I need a check here to compare against an existing model. Not sure yet how to access a current model. 
    # Should we have a file on gs that tracks the auc score of the trained models? Or the link to the current model
    # The threshold should also be available somewhere and not hardcoded. 

    return (deploy,)

if __name__ == "__main__": 

    import kfp

    kfp.components.create_component_from_func(
        model_evaluation,
        output_component_file='model_evaluation.yaml', 
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
        packages_to_install=["pandas", "sklearn", "xgboost"])