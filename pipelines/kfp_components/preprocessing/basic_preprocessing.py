
from kfp.v2.dsl import Artifact


def basic_preprocessing(
    input_file: str,
    output_bucket: str,
    output_file: str
) -> Artifact: 

    from functools import reduce

    import pandas as pd

    df = pd.read_csv(input_file)

    df.columns = [col.lower().strip() for col in df.columns] 

    df.dropna(inplace=True)
    
    df.rename({'default_payment_next_month': "default"}, inplace=True)
    df.loc[df['education'] == '0', 'education'] = 'Unknown'
    df.loc[df['marriage'] == '0', 'marriage'] = 'Other'
    sex = pd.get_dummies(df.sex, prefix='gender')
    education = pd.get_dummies(df.education, prefix='ed')
    marriage = pd.get_dummies(df.marriage, prefix='mstatus')
    frames = [df, sex, education, marriage]
    final = reduce(lambda l, r: pd.concat([l, r], axis=1), frames)
    final.drop(['default_payment_next_month', 'sex', 'education', 'marriage'], axis=1, inplace=True)

    output_path = f"gs://mle-dwh-torus/{output_bucket}/{output_file}" 
    final.to_csv(output_path, index=False)

    return final

if __name__ == "__main__": 
    
    import kfp 

    kfp.components.func_to_container_op(
        basic_preprocessing,
        extra_code="from kfp.v2.dsl import Artifact, Dataset, InputPath, OutputPath",
        output_component_file='basic_preprocessing_component.yaml', 
        base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
        packages_to_install=["fsspec", "gcsfs"]
    )
