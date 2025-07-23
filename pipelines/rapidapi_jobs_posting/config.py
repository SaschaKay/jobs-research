from key import API_KEY
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

# query parameters for the API

URL = (
    # request_type can be "search" or "count"
    # search - get job postings page by page, default is 10 items per page
    # count - get total count of job postings
    "https://daily-international-job-postings.p.rapidapi.com/api/v2/jobs/{request_type}"
)

DATE_CREATED = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")

QUERYPARAMS = {
    "dateCreated":  DATE_CREATED, # date format YYYY-MM or YYYY-MM-DD
    "countryCode":  "de",
    "title":        "Data",
    # "city":       "Munich",
    "language":     "en",
    "locale":       "en_DE",
}

MONTH_CREATED = QUERYPARAMS["dateCreated"].replace("-", "_")[:7]
DATE_CREATED = QUERYPARAMS["dateCreated"].replace("-", "_")

HEADERS = {
    "x-rapidapi-key":  API_KEY,
    "x-rapidapi-host": "daily-international-job-postings.p.rapidapi.com",
}


START_PAGE = 1  # the number of the first page to fetch
# set N if the pipeline failed on N requests
END_PAGE = 1  # the number of the last page to fetch
# None -> calculate max_page based on the total count of job postings
# max_page = 2 -> set max_page to 2 for testing on a small number of requests


# Google Cloud parameters

SERVER = "dev" # can be "dev" or "prod"

GCP_NAME = {
    'dev':  "x-avenue-450615-c3",
    'prod': "x-avenue-450615-c3"
    }

GCS_PARAMS = {
    "dev": {
        "bucket":       "de-zoomcamp-2025-dev-terra-bucket",
        "storage_path": "raw/jobs/",
        "file_name":    f"rapidapi_test2/{MONTH_CREATED}/{DATE_CREATED}_page", 
    },
    "prod": {
        "bucket":       "jobs-postings-bucket-prod",
        "storage_path": "raw/jobs/",
        "file_name":    f"rapidapi/{MONTH_CREATED}/{DATE_CREATED}_page",
    },
}

BQ_DWH_PARAMS = {
    "dev":  {"dataset_name": "jobs_postings_test", "location": "europe-west1"},
    "prod": {"dataset_name": "jobs_postings",      "location": "europe-west1"},
}

BQ_ADB_PARAMS = {
    "dev":  {"dataset_name": "jp_test", "location": "europe-west1"},
    "prod": {"dataset_name": "jp",      "location": "europe-west1"},
}

#data transform parameters

JOBS_POSTINGS_FINAL_COLS = [
    "id",
    "date_created",
    "company",
    #"city_group",
    #"position",
    "portal",
    "url",
    "years_of_experience",
    "description",
    "city_clusters",
    "positions",
    "skills"
]

MAPPING_RULES_EXPORT_URLS = {
    "skills": 
        "https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=2082815936#gid=2082815936",
    "city_clusters": 
        "https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=217199346#gid=217199346",
    "positions": 
        "https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=1908800533#gid=1908800533",
}