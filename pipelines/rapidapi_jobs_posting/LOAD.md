# 1. API requests

Load job daily requests job postings via the side API. It uses parameters `countryCode`, `title`, `language`, and `locale` to request only postings for English-speaking specialists in Germany with the word "data" in the title and `date_created` as a date filter.
Request parameters live in `pipelines/rapidapi_jobs_posting/config.py`.

### Useful links

* [Example responses and code snippets](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings/playground/apiendpoint_e1e15a81-2b65-4802-92cf-f300e7f59558)
* [Documentation](https://api.techmap.io/jobs-api)
* [API home page on rapidapi.com](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings)

### Count endpoint

The count endpoint is used to define the number of pages to be requested based on the `totalCount` field. The number of postings per page depends on the API subscription type, currently 10.

### Search endpoint

The response contains parsed postings. Though in addition to raw data, the response also includes some fields normalized by the API provider, most of them are not suitable for our analysis (look at `TRANSFORM.md` for a list of used fields, and `explore/rapidapi_jobs_posting_cleaning.ipynb` for detailed reasoning).

# 2. [dlt](https://dlthub.com/) load

At the moment of implementation, some pages of the DLT documentation claimed that it can work with GCP out of the box, while some claimed that it can't. Experiments with connecting GCP weren't successful, so a custom paginated source was written (`paginated source` from `common\utils.py`).





