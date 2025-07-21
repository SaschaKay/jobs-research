import sys
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# adding paths for project modules
CUR_DIR_WARNING = (
    "__file__ was not available, os.getcwd() was used instead. "
    "You may need to change the working directory."
)
try:
    CURRENT_DIRECTORY = os.path.dirname(__file__)
except NameError:
    CURRENT_DIRECTORY = os.getcwd()
    logging.debug(CUR_DIR_WARNING)

if CURRENT_DIRECTORY not in sys.path:
    sys.path.append(CURRENT_DIRECTORY)
    
from config import PROJECT_ROOT_RELATIVE
PROJECT_ROOT = os.path.abspath(
    os.path.join(CURRENT_DIRECTORY, PROJECT_ROOT_RELATIVE)
)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    logging.debug(f"{PROJECT_ROOT} was appended to sys.path")

from datetime import timedelta
from copy import deepcopy
import dlt

from common.utils import (
    paginated_source,
    get_gcp_key,
)

from functions import flattened_jobs_posting, count_pages


def calculate_creation_date(execution_date, delta_days):
    date_created = (execution_date - timedelta(days=delta_days)).strftime("%Y-%m-%d")
    month_created_folder = date_created.replace("-", "_")[:7]
    date_created_folder = date_created.replace("-", "_")
    return date_created, month_created_folder, date_created_folder

def get_end_page(url, headers, query_params, end_page=None)-> int:
    """
    Get the maximum number of pages for the job postings.
    If end_page is not defined, it will be calculated based on the total count of job postings.
    If end_page is defined, it will be used as is.
    """
    if end_page != 1:
        queryparams_parquet = deepcopy(query_params)
        queryparams_parquet["format"] = "parquet"
        max_page = count_pages(
            url.format(request_type="count"), queryparams=queryparams_parquet, headers=headers
        )
        if end_page is None:
            end_page = max_page 
        else:
            end_page = min(max_page, end_page) 
    return end_page


def main(**kwargs):
    params = kwargs["params"]
    logger.info(f"Server: {params['server']}")
    execution_date = kwargs["execution_date"]
    logger.info(f"Execution Date: {execution_date}")

    gcp_params = params["gcp"]
    queryparams = params["queryparams"]
    headers = params["query_headers"]
    url = params["url"]

    query_settings = params["query_settings"]
    date_created_delta_days = query_settings["date_created_delta_days"]
    start_page = query_settings["start_page"]
    end_page = query_settings["end_page"]

    date_created, month_created_folder, date_created_folder = calculate_creation_date(execution_date, date_created_delta_days)
    queryparams["dateCreated"] = date_created
    end_page = get_end_page(url, headers, queryparams, end_page)

    logger.info(f'Pages from {start_page} to {end_page} will be requested')
    logger.info("Getting postings data...")

    #Creating source for dlt pipeline
    source = paginated_source(
        url=url.format(request_type="search"),
        response_format="json",
        queryparams=queryparams,
        headers=headers,
        start_page=start_page,
        end_page=end_page,
        upload_to_gcs=True, #upload raw data to GCS
        gcs_bucket=gcp_params["gcs_bucket"],
        storage_path=gcp_params["gcs_storage_path"],
        file_name=gcp_params["gcs_file_name_pattern"].format(month_created=month_created_folder, date_created=date_created_folder),
    )
        
    #Get job postings from RapidAPI, upload raw data to Google Cloud Storage (GCS), and normalized data to BigQuery.
    pipeline = dlt.pipeline(
        pipeline_name="job_postings_to_bq_pipeline",
        destination=dlt.destinations.bigquery(
            credentials=get_gcp_key(),
            dataset_name=gcp_params["bq_dwh_params"]["dataset_name"],
            location=gcp_params["bq_dwh_params"]["location"]
        )
    )

    pipeline_info = pipeline.run(
        flattened_jobs_posting(source),
    )

    logger.info(str(pipeline_info))