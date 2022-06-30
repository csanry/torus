
from kfp.v2.dsl import HTML, Output, component


@component(
    base_image="gcr.io/pacific-torus-347809/mle-fp/preprocessing:v1",
    output_component_file="tfdv_visualise_statistics_component.yaml"
)
def tfdv_visualise_statistics(
    statistics_path: str,  
    output_view: Output[HTML],
    output_bucket: str = "mle-dwh-torus/stats",
    statistics_name: str = "",
    older_statistics_name: str = "",
    older_statistics_path: str = "",
) -> None:  

    # import packages
    from datetime import datetime

    import tensorflow_data_validation as tfdv
    from tensorflow_data_validation.utils.display_util import \
        get_statistics_html

    # load stats from path
    stats = tfdv.load_statistics(input_path=statistics_path)
    older_stats = None
    if older_statistics_path:
        older_stats = tfdv.load_statistics(input_path=older_statistics_path)

    # create html file to statistics
    html = get_statistics_html(
        lhs_statistics=stats,
        lhs_name=statistics_name,
        rhs_statistics=older_stats,
        rhs_name=older_statistics_name,
    )

    # write html to output view
    stats_id = datetime.now().strftime(f"%Y-%m")

    file_path = f"{output_bucket}/credit-card-default-stats-{stats_id}.html"
    if not file_path.startswith("gs://"):
        file_path = f"gs://{file_path}"
    output_view.path = file_path

    with open(output_view.path, "w") as file:
        file.write(html)
