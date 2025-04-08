import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from copy import deepcopy
import dlt

from common.utils import (
    paginated_source,
    get_gcp_key,
)
from config import ( 
    #request parameters
    START_PAGE, #set i
    END_PAGE, 
    QUERYPARAMS, 
    HEADERS, 
    URL,
    #destinations parameters
    GCS_PARAMS,
    BQ_PARAMS,
    #switching between test/prod parameters
    SERVER
)
from functions import flattened_jobs_posting, count_pages


def get_end_page()-> int:
    """
    Get the maximum number of pages for the job postings.
    If end_page is not defined, it will be calculated based on the total count of job postings.
    If end_page is defined, it will be used as is.
    """
    if END_PAGE ==1: 
        end_page = END_PAGE
    else:
        queryparams_parquet = deepcopy(QUERYPARAMS)
        queryparams_parquet["format"] = "parquet"
        max_page = count_pages(
            URL.format(request_type="count"), queryparams=queryparams_parquet, headers=HEADERS
        )
        end_page = max_page if END_PAGE is None else min(max_page, END_PAGE) 
 
    print(f'Pages from {START_PAGE} to {end_page} will be requested')
    return end_page

def rapidapi_jobs_posting(end_page: int = 1) -> None:
    """
    Get job postings from RapidAPI, upload raw data to Google Cloud Storage (GCS), and normalized data to BigQuery.
    The function uses the dlt library to create a pipeline that extracts job postings from the RapidAPI service.
    """
    print("")
    for key, val in QUERYPARAMS.items():
        print(f"{key}: {val}")
    print("")
    
    source = paginated_source(
        url=URL.format(request_type="search"),
        response_format="json",
        queryparams=QUERYPARAMS,
        headers=HEADERS,
        start_page=START_PAGE,
        end_page=end_page,
        upload_to_gcs=True, #upload raw data to GCS
        gcs_bucket=GCS_PARAMS[SERVER]["bucket"],
        storage_path=GCS_PARAMS[SERVER]["storage_path"],
        file_name=GCS_PARAMS[SERVER]['file_name'],
    )

    pipeline = dlt.pipeline(
        pipeline_name="job_postings_to_bq_pipeline",
        destination="bigquery",
        dataset_name=BQ_PARAMS[SERVER]['dataset_name'],  # will be created if not excists
    )


    pipline_info = pipeline.run(
        flattened_jobs_posting(source),
        credentials=get_gcp_key(),
    )

    print(pipline_info)



def main():
    rapidapi_jobs_posting(get_end_page())

if __name__=="__main__":
    main()