import os

# query parameters for the API

URL = (
    # request_type can be "search" or "count"
    # search - get job postings page by page, default is 10 items per page
    # count - get total count of job postings
    "https://daily-international-job-postings.p.rapidapi.com/api/v2/jobs/{request_type}"
)

QUERYPARAMS = {
    "dateCreated": "2025-01-20",  # date format YYYY-MM or YYYY-MM-DD
    "countryCode": "de",
    "title": "Data",
    # "city": "Munich",
    "language": "en",
}

MONTH_CREATED = QUERYPARAMS["dateCreated"].replace("-", "_")[:7]
DATE_CREATED = QUERYPARAMS["dateCreated"].replace("-", "_")

HEADERS = {
    "x-rapidapi-key": "24eedad401msh992a0a2dccd6d5dp178930jsnbd454e2cb868",
    "x-rapidapi-host": "daily-international-job-postings.p.rapidapi.com",
}


START_PAGE = 1  # the number of the first page to fetch
# set N if the pipeline failed on N requests
END_PAGE = None  # the number of the last page to fetch
# None -> calculate max_page based on the total count of job postings
# max_page = 2 -> set max_page to 2 for testing on a small amount of requests


# Google Cloud parameters

SERVER = os.environ["SERVER_TYPE"] # can be "dev" or "prod"
# defines the GCS bucket and BQ dataset to use 
# set in ~/.profile

GCS_PARAMS = {
    "dev": {
        "bucket": "de-zoomcamp-2025-dev-terra-bucket",
        "storage_path": "raw/jobs/",
        "file_name": f"rapidapi_test2/{MONTH_CREATED}/page",
    },
    "prod": {
        "bucket": "jobs-postings-bucket-prod",
        "storage_path": "raw/jobs/",
        "file_name": f"rapidapi/{MONTH_CREATED}/{DATE_CREATED}_page",
    },
}

BQ_PARAMS = {
    "dev": {"dataset_name": "job_postings_test", "location": "US"},
    "prod": {"dataset_name": "jobs_postings", "location": "europe-west1"},
}
