
from datetime import datetime

import google.cloud.aiplatform as aip
import kfp
from google.cloud import storage
from kfp.v2 import compiler, dsl

tfdv_op = kfp.components.load_component_from_file("./pipelines/kfp_components/preprocessing/tfdv_generate_statistics_component.yaml")

def test_tfdv_generate_statistics():

    def test_generate_statistics_pipeline():
        
        tfdv_op(
            input_data="gs://mle-dwh-torus/int/ccd2.csv", # basic_preprocessing.output,
            output_path="gs://mle-dwh-torus/tfdv_expers/eval/evaltest.pb",
            job_name='test-1',
            use_dataflow="False",
            project_id="pacific-torus-347809",
            region="us-central1", # us-central1
            gcs_temp_location='gs://mle-dwh-torus/tfdv_expers/tmp',
            gcs_staging_location='gs://mle-dwh-torus/tfdv_expers',
            whl_location="tensorflow_data_validation-0.26.0-cp37-cp37m-manylinux2010_x86_64.whl"
        )

    aip.init(
        project="pacific-torus-347809", 
        staging_bucket="gs://mle-dwh-torus"
    )

    id = datetime.now().strftime(f"%d%H%M")

    compiler.Compiler().compile(
        pipeline_func=test_generate_statistics_pipeline,
        pipeline_name="test-generate-statistics-train-pipeline",
        package_path="./test.json",
        type_check=True,
    )

    job = aip.PipelineJob(
        display_name="testpipeline",
        template_path="./test.json",
        job_id=f'test-{id}',
        pipeline_root="gs://mle-dwh-torus/pipeline/"
    )

    job.run()

    job.delete()

    storage_client = storage.Client(project="pacific-torus-347809")
    bucket = storage_client.bucket(bucket_name="mle-dwh-torus")

    test_path = "tfdv_expers/eval/evaltest.pb"

    blob = storage.Blob(name=test_path, bucket=bucket)

    check_exists = blob.exists(storage_client)

    assert check_exists



test_tfdv_generate_statistics()
