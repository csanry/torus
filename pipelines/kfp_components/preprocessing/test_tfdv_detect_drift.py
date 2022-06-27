

from pipelines.kfp_components.preprocessing.tfdv_detect_drift import \
    tfdv_detect_drift


def test_tfdv_detect_drift():

    import logging
    import time

    import tensorflow_data_validation as tfdv
    import tensorflow_data_validation.statistics.stats_impl

    check_return_true = tfdv_detect_drift(
        stats_older_path="none", 
        stats_new_path="placeholder", 
        target_feature="limit_bal"
    )

    assert check_return_true == ("true",)


