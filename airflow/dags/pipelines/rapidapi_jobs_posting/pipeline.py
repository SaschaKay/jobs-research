from copy import deepcopy
import dlt

from pipelines.common.utils import (
    count_pages,
    paginated_source,
)
from config import (
    max_page, 
    queryparams, 
    headers, 
    url, 
    gcs_params,
    server
)
from functions import flattened_jobs_posting


def get_jobs_posting():

    if max_page is None:
        
        queryparams_parquet = deepcopy(queryparams)
        queryparams_parquet["format"] = "parquet"

        max_page = count_pages(
            url.format(request_type="count"), queryparams=queryparams_parquet, headers=headers
        )


    source = paginated_source(
        url=url.format(request_type="search"),
        response_format="json",
        queryparams=queryparams,
        headers=headers,
        upload_to_gcs=True,
        gcs_bucket="de-zoomcamp-2025-dev-terra-bucket",
        storage_path="raw/jobs/",
        file_name=f"rapidapi_test2/{queryparams['dateCreated']}/page",
        max_page=max_page,
    )

    pipeline = dlt.pipeline(
        pipeline_name="job_postings_to_bq_pipeline4",
        destination="bigquery",
        dataset_name="job_postings",  # будет создан в BQ, если его ещё нет
        import_schema_path="schemas/import4",
        export_schema_path="schemas/export4",
    )


    pipline_info = pipeline.run(
        flattened_jobs_posting(source),
        credentials=gcs_params[server],
    )

    print(pipline_info)