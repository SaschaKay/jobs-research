url = (
    "https://daily-international-job-postings.p.rapidapi.com/api/v2/jobs/{request_type}"
)

queryparams = {
    "dateCreated": "2025-02",
    "countryCode": "de",
    "title": "Data",
    "city": "Munich",
    "language": "en",
}

headers = {
    "x-rapidapi-key": "24eedad401msh992a0a2dccd6d5dp178930jsnbd454e2cb868",
    "x-rapidapi-host": "daily-international-job-postings.p.rapidapi.com",
}

max_page = None

server = "dev"

gcs_params = {
    'dev': {
        'bucket': "de-zoomcamp-2025-dev-terra-bucket",
        'storage_path': "raw/jobs/",
        'file_name': f"rapidapi_test2/{queryparams['dateCreated']}/page"
    }
}

