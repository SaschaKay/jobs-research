import sys
import os

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PROJECT_ROOT_RELATIVE = "../../"
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

PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIRECTORY, PROJECT_ROOT_RELATIVE))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    logging.debug(f"{PROJECT_ROOT} was appended to sys.path")

from typing import Any, Literal
from common.utils import check_literal_values
from key import API_KEY


# Defines params for a DAG
ParamsServer = Literal["dev", "prod"]


class PipelineParams:
    # query parameters for the API
    QUERY_HEADERS = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "daily-international-job-postings.p.rapidapi.com",
    }

    # request_type can be "search" or "count"
    # search - get job postings page by page, default is 10 items per page
    # count - get total count of job postings
    URL = "https://daily-international-job-postings.p.rapidapi.com/api/v2/jobs/{request_type}"

    STATIC_QUERYPARAMS = {
        "countryCode": "de",
        "title": "Data",
        # "city": "Munich",
        "language": "en",
        "locale": "en_DE",
        "dateCreated": "",  # will be calculated based on dag's execution date and DATE_CREATED_DELTA_DAYS
    }
    QUERY_SETTINGS = {
        "start_page": 1,  # the number of the first page to fetch
        # set N if the pipeline failed on N requests
        "end_page": {
            # the number of the last page to fetch
            # None -> calculate max_page based on the total count of job postings
            # max_page = 2 -> set max_page to 2 for testing on a small number of requests
            "dev": 2,
            "prod": None,
        },
        "date_created_delta_days": 7,
    }

    # Google Cloud parameters
    GCP_PARAMS = {
        "project": {"dev": "x-avenue-450615-c3", "prod": "x-avenue-450615-c3"},
        "gcs_bucket": {
            "dev": "de-zoomcamp-2025-dev-terra-bucket",
            "prod": "jobs-postings-bucket-prod",
        },
        "gcs_storage_path": {"dev": "raw/jobs/", "prod": "raw/jobs/"},
        "gcs_file_name_pattern": {
            "dev": "rapidapi_test2/{month_created}/{date_created}_page",
            "prod": "rapidapi/{month_created}/{date_created}_page",
        },
        "bq_dwh_params": {
            "dev": {"dataset_name": "jobs_postings_test", "location": "europe-west1"},
            "prod": {"dataset_name": "jobs_postings", "location": "europe-west1"},
        },
        "bq_adb_params": {
            "dev": {"dataset_name": "jp_test", "location": "europe-west1"},
            "prod": {"dataset_name": "jp", "location": "europe-west1"},
        },
    }

    # Final DataFrames' columns to upload in BQ
    FINAL_DFS_COLS = {
        "jobs": [
            "id",
            "date_created",
            "company",
            # "city_group",
            # "position",
            "portal",
            "url",
            "years_of_experience",
            "description",
            "city_clusters",
            "positions",
            "skills",
        ]
    }

    MAPPING_RULES_URLS = {
        "skills": "https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=2082815936#gid=2082815936",
        "city_clusters": "https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=217199346#gid=217199346",
        "positions": "https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=1908800533#gid=1908800533",
    }

    def __init__(self, server: ParamsServer):
        check_literal_values(server, "server", ParamsServer)
        self.server = server

    def resolve_env_config(self, val: Any):
        if isinstance(val, dict):
            if self.server in val.keys():
                return val[self.server]
        return val

    def get_params(self):
        params = {"server": self.server}

        params["query_headers"] = self.QUERY_HEADERS
        params["url"] = self.URL
        params["queryparams"] = self.STATIC_QUERYPARAMS

        params["query_settings"] = {}
        for key, val in self.QUERY_SETTINGS.items():
            params["query_settings"][key] = self.resolve_env_config(val)

        params["gcp"] = {}
        for key, val in self.GCP_PARAMS.items():
            params["gcp"][key] = self.resolve_env_config(val)

        params["mapping_rules_urls"] = self.MAPPING_RULES_URLS
        params["final_df_cols"] = self.FINAL_DFS_COLS

        self.params = params
        return params
