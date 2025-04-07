from copy import deepcopy
import dlt

from pipelines.common.utils import (
    count_pages,
    paginated_source,
    get_gcp_key,
)
from config import (
    MAX_PAGE, 
    MIN_PAGE,
    QUERYPARAMS, 
    HEADERS, 
    URL, 
    GCS_PARAMS,
    SERVER
)
from functions import flattened_jobs_posting


def get_max_page()-> int:
    """
    Get the maximum number of pages for the job postings.
    If max_page is not defined, it will be calculated based on the total count of job postings.
    If max_page is defined, it will be used as is.
    """
    if MAX_PAGE is not None:
        max_page = MAX_PAGE
    else:
        queryparams_parquet = deepcopy(QUERYPARAMS)
        queryparams_parquet["format"] = "parquet"
        max_page = count_pages(
            URL.format(request_type="count"), queryparams=queryparams_parquet, headers=HEADERS
        )
    return max_page

def rapidapi_jobs_posting(max_page: int = 1) -> None:
    """
    Get job postings from RapidAPI, upload raw data to Google Cloud Storage (GCS) and normalized data to BigQuery.
    The function uses the dlt library to create a pipeline that extracts job postings from the RapidAPI service.
    """
    source = paginated_source(
        url=URL.format(request_type="search"),
        response_format="json",
        queryparams=QUERYPARAMS,
        headers=HEADERS,
        max_page=max_page,
        min_page=MIN_PAGE,
        upload_to_gcs=True, #upload raw data to GCS
        gcs_bucket=GCS_PARAMS[SERVER]["bucket"],
        storage_path=GCS_PARAMS[SERVER]["storage_path"],
        file_name=GCS_PARAMS[SERVER]['file_name'].format(date_created=QUERYPARAMS["dateCreated"]),
    )

    pipeline = dlt.pipeline(
        pipeline_name="job_postings_to_bq_pipeline",
        destination="bigquery",
        dataset_name="job_postings",  # будет создан в BQ, если его ещё нет
        import_schema_path="schemas/import",
        export_schema_path="schemas/export",
    )


    pipline_info = pipeline.run(
        flattened_jobs_posting(source),
        credentials=get_gcp_key(),
    )

    print(pipline_info)