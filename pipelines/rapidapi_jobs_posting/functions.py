def count_pages(
    url: str,
    queryparams: dict = None,  
    headers: dict = None,
    items_per_page: int = 10,
):
    response_count = response = requests.get(
        url, headers=headers or {}, params=queryparams or {}
    )

    parquet_bytes = response_count.content
    buffer = BytesIO(parquet_bytes)

    table = pq.read_table(buffer)
    df = table.to_pandas()

    jobs_count = df["totalCount"][0]
    max_page = ceil(jobs_count / items_per_page)
    return max_page

@dlt.resource(write_disposition="append", table_name="jobs_posting")
def flattened_jobs_posting(source):
    for record in source.resources["get_pages"]():
        yield flatten_dict_by_key(nested_dict=record, keys=["jsonLD"])