This document explains how raw job postings are fetched from **API**, stored in **Google Cloud Storage**, and loaded into **BigQuery** using dlt.

For BD schema see [SCHEMA.md](./SCHEMA.md)

# Configuration

[config.py](./config.py) defines:
* API access headers, base URL, and static query parameters,
* Google Cloud settings – project, bucket, storage path, file name pattern,
* BigQuery dataset/location.
It also handles per-environment settings, such as start and end pages, or how many days back to request via `date_created_delta_days`.
Parameters may differ for prod and dev environments. Class `PipelineParams` resolves the proper values for the current environment (`ENV` variable pulled by Airflow dag) when building the parameter dictionary. 

# API requests

Load job daily requests job postings via the Techmap.io API. This API was chosen for its flexible filters and the ability to pay per request without an additional fixed subscription fee, which significantly optimizes the budget. 
Parameters `countryCode`, `title`, `language`, and `locale` are used to request only postings for English-speaking specialists in Germany with the word "data" in the title and `dateCreated` as a date filter.
Request parameters live in [config.py](./config.py).

### Useful links

* [Example responses and code snippets](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings/playground/apiendpoint_e1e15a81-2b65-4802-92cf-f300e7f59558)
* [Documentation](https://api.techmap.io/jobs-api)
* [API home page on rapidapi.com](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings)

### Count endpoint

The count endpoint is used to define the number of pages to be requested based on the `totalCount` field. The number of postings per page depends on the API subscription type, currently 10.

### Search endpoint

The response contains parsed postings. Though in addition to raw data, the response also includes some fields normalized by the API provider, most of them are not suitable for our analysis (look at [`TRANSFORM.md`](./TRANSFORM.md) for a list of used fields, and [`explore/rapidapi_jobs_posting_cleaning.ipynb`](./../../explore/rapidapi_jobs_posting_cleaning.ipynb) for detailed reasoning).

# [dlt](https://dlthub.com/) pipeline

## Source definition
At the moment of implementation, some pages of the DLT documentation claimed that it can work with GCP out of the box, while some claimed that it can't. Experiments with connecting GCP weren't successful, so a custom paginated source was written.
`paginated_source` in [`utils.py`](./../../common/utils.py) is a custom paginated source under decorator `@dlt.source` that yields pages from the API and can upload each raw response (JSON) to GCS (used as a backup).
`paginated_source` is API-agnostic and can be reused.

## dlt reource
The dlt pipeline targets BigQuery. dlt handles 
* tables [schema](https://dlthub.com/docs/general-usage/schema-contracts), including datatypes and creating child tables, schema evolution, unique keys creation
* basic normalization, like extra space deletion, fixing types according to the schema, etc.,
* chunk processing, if needed,
* logging of processed loads, pipeline state and schema evolution.
Flattening is partly handled by `flattened_jobs_posting`, partly with dlt(flatten one level to child tables in star schema).









