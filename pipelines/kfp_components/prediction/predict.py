from typing import NamedTuple

from kfp.components import InputPath
from kfp.v2.dsl import Artifact


def predict(
        model_input_file: InputPath("BST"),
        serving_container_image_uri: str,
        project_id : str,
        region: str
) -> NamedTuple("outputs", [
    # ("prediction", str),
    ("vertex_endpoint", Artifact),
    ("vertex_model", Model)
]):

    from google.cloud import aiplatform
    aiplatform.init(project=project_id, location=region)

    DISPLAY_NAME  = "Credit-Card-Default-Check"
    MODEL_NAME = "CCD-XGB"
    ENDPOINT_NAME = "Credit-Card-Default-Check-Endpoint"

    def create_endpoint():
        endpoints = aiplatform.Endpoint.list(
            filter='display_name="{}"'.format(ENDPOINT_NAME),
            order_by='create_time desc',
            project=project_id,
            location=region,
        )
        if len(endpoints) > 0:
            endpoint = endpoints[0]
        else:
            endpoint = aiplatform.Endpoint.create(
                display_name=ENDPOINT_NAME, project=project_id, location=region
            )

    endpoint = create_endpoint()

    model_upload = aiplatform.Model.upload(
        display_name = DISPLAY_NAME,
        artifact_uri = model_input_file,
        serving_container_image_uri =  serving_container_image_uri,
        serving_container_health_route=f"/v1/models/{MODEL_NAME}",
        serving_container_predict_route=f"/v1/models/{MODEL_NAME}:predict",
        serving_container_environment_variables={
        "MODEL_NAME": MODEL_NAME,
        }

    model_deploy = model_upload.deploy(
        machine_type="n1-standard-4",
        endpoint=endpoint,
        traffic_split={"0": 100},
        deployed_model_display_name=DISPLAY_NAME,
    )

    vertex_model.uri = model_deploy.resource_name
    # from collections import namedtuple
    #
    # results = namedtuple("outputs", ["train_data", "test_data"])
    #
    # return results(train, test)


if __name__ == "__main__":
    import kfp

    kfp.components.create_component_from_func(
        predict,
        output_component_file='predict.yaml',
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
        packages_to_install=["fsspec", "gcsfs"])
