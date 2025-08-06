This document explains how raw job postings are fetched from API, stored in Google Cloud Storage, and loaded into BigQuery using dlt.

# 0. Configuration

[config.py](./pipelines/rapidapi_jobs_posting/config.py) defines:
* API access headers, base URL, and static query parameters
* Google Cloud settings – project, bucket, storage path, file name pattern
* BigQuery dataset/location
also handles per-environment settings such as start and end pages or how many days back to request via `date_created_delta_days`.
Parameters may differ for prod and dev environments. Class `PipelineParams` resolves the proper values for the current environment (`ENV` variable pulled by Airflow dag) when building the parameter dictionary. 

# 1. API requests

Load job daily requests job postings via the side API. It uses parameters `countryCode`, `title`, `language`, and `locale` to request only postings for English-speaking specialists in Germany with the word "data" in the title and ``dateCreated`` as a date filter.
Request parameters live in [config.py](./pipelines/rapidapi_jobs_posting/config.py).

### Useful links

* [Example responses and code snippets](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings/playground/apiendpoint_e1e15a81-2b65-4802-92cf-f300e7f59558)
* [Documentation](https://api.techmap.io/jobs-api)
* [API home page on rapidapi.com](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings)

### Count endpoint

The count endpoint is used to define the number of pages to be requested based on the `totalCount` field. The number of postings per page depends on the API subscription type, currently 10.

### Search endpoint

The response contains parsed postings. Though in addition to raw data, the response also includes some fields normalized by the API provider, most of them are not suitable for our analysis (look at `TRANSFORM.md` for a list of used fields, and `explore/rapidapi_jobs_posting_cleaning.ipynb` for detailed reasoning).

# 2. [dlt](https://dlthub.com/) pipeline

## Source definition
At the moment of implementation, some pages of the DLT documentation claimed that it can work with GCP out of the box, while some claimed that it can't. Experiments with connecting GCP weren't successful, so a custom paginated source was written.
[`paginated_source`](./common/utils.py) is a custom `@dlt.source` that yields pages from the API and can upload each raw response (json) to GCS. Files with responses are used as a backup.

## Load workflow
The dlt pipeline `job_postings_to_bq_pipeline` targets BigQuery and uses credentials supplied by `get_gcp_key`. 
Flattening is partly handled by `flattened_jobs_posting`, partly with dlt(can only flatten one level). dlt also handles basic cleaning, like truncation, etc. A `dlt.resource` writes to the table `jobs_posting` ADDD HEREeeeeEEEEEEE with `append`.








