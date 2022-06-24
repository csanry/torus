from typing import NamedTuple


def tfdv_detect_drift(
    stats_older_path: str, stats_new_path: str, target_feature: str
) -> NamedTuple('Outputs', [('drift', str)]):

    import logging
    import time

    import tensorflow_data_validation as tfdv
    import tensorflow_data_validation.statistics.stats_impl

    logging.getLogger().setLevel(logging.INFO)
    logging.info('stats_older_path: %s', stats_older_path)
    logging.info('stats_new_path: %s', stats_new_path)

    # if there are no older stats to compare with, just return 'true'
    if stats_older_path == 'none':
        return ('true', )

    stats1 = tfdv.load_statistics(stats_older_path)
    stats2 = tfdv.load_statistics(stats_new_path)

    schema1 = tfdv.infer_schema(statistics=stats1)
    
    tfdv.get_feature(schema1, target_feature).drift_comparator.jensen_shannon_divergence.threshold = 0.01
    
    drift_anomalies = tfdv.validate_statistics(
        statistics=stats2, schema=schema1, previous_statistics=stats1)
    logging.info('drift analysis results: %s', drift_anomalies.drift_skew_info)

    from google.protobuf.json_format import MessageToDict
    
    d = MessageToDict(drift_anomalies)
    
    val = d['driftSkewInfo'][0]['driftMeasurements'][0]['value']
    
    thresh = d['driftSkewInfo'][0]['driftMeasurements'][0]['threshold']
    logging.info('value %s and threshold %s', val, thresh)
    
    res = 'true'
    
    if val < thresh:
        res = 'false'
    logging.info(f'train decision: {res}')
    return (res, )


import kfp

kfp.components.create_component_from_func(
    tfdv_detect_drift,
    output_component_file='tfdv_detect_drift_component.yaml', 
    base_image='gcr.io/pacific-torus-347809/mle-fp/preprocessing:latest')

print("done")
