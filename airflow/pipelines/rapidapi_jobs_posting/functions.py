import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from io import BytesIO
from math import ceil
import pyarrow.parquet as pq
import re
from typing import Iterable 

import dlt
from dlt.sources.helpers import requests

from common.utils import flatten_dict_by_key


def count_pages(
    url: str,
    queryparams: dict = None,  
    headers: dict = None,
    items_per_page: int = 10,
):
    response_count = requests.get(
        url, headers=headers or {}, params=queryparams or {}
    )

    parquet_bytes = response_count.content
    buffer = BytesIO(parquet_bytes)
    table = pq.read_table(buffer)
    df = table.to_pandas()

    jobs_count = df["totalCount"][0]
    max_pages = ceil(jobs_count / items_per_page)

    print(f'{jobs_count} posts found by request, the maximum amount of pages are {max_pages}')
    
    return max_pages

@dlt.resource(write_disposition="append", table_name="jobs_posting")
def flattened_jobs_posting(source):
    for record in source.resources["get_pages"]():
        yield flatten_dict_by_key(nested_dict=record, keys=["jsonLD"])


def find_position_in_text(
    texts: Iterable,
    mapping_dict: dict,
) -> str:
    for text in texts:
        for key, val in mapping_dict.items():
            if key in text:
                return val
    return None


def collapse_city_groups(city_name: str, city_clusters: dict) -> str:
    if not isinstance(city_name, str):
        return "Other"
    city_lc = re.sub("[^a-zA-Z]+", "", city_name).lower()
    for region, keywords in city_clusters.items():
        if any(re.sub("[^a-zA-Z]+", "", keyword) in city_lc for keyword in keywords):
            return region
    return "Other"