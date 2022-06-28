
def basic_preprocessing(
    input_file: str,
    output_bucket: str,
    output_file: str,
) -> str: 

    from functools import reduce

    import pandas as pd

    df = pd.read_csv(input_file)

    df.columns = [col.lower().rstrip('_') for col in df.columns] 

    df.dropna(inplace=True)
    
    df['target'] = df['default'].apply(lambda x: 1 if x == True else 0)
    df.loc[df['education'] == '0', 'education'] = 'Unknown'
    df.loc[df['marriage'] == '0', 'marriage'] = 'Other'
    sex = pd.get_dummies(df.sex, prefix='gender')
    education = pd.get_dummies(df.education, prefix='ed')
    marriage = pd.get_dummies(df.marriage, prefix='mstatus')
    frames = [df, sex, education, marriage]
    final = reduce(lambda l, r: pd.concat([l, r], axis=1), frames)
    final.drop(['id', 'default', 'sex', 'education', 'marriage'], axis = 1, inplace = True)

    output_path = f"gs://mle-dwh-torus/{output_bucket}/{output_file}" 
    final.to_csv(output_path, index=False)

    return output_path

import kfp

kfp.components.func_to_container_op(
    basic_preprocessing,
    extra_code="from kfp.v2.dsl import Artifact, Dataset, InputPath, OutputPath",
    output_component_file='basic_preprocessing_component.yaml', 
    base_image='gcr.io/pacific-torus-347809/mle-fp/base:latest',
    packages_to_install=["fsspec", "gcsfs"]
)

print('done')
