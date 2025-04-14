import sys
import os

CURRENT_DIRECTORY = os.path.abspath(__file__)
sys.path.append(CURRENT_DIRECTORY)

from config import PROJECT_ROOT_RELATIVE
sys.path.append(
    os.path.abspath(
        os.path.join(CURRENT_DIRECTORY, PROJECT_ROOT_RELATIVE)
    )
)

from io import BytesIO
import hashlib
from math import ceil
import pyarrow.parquet as pq
import re
from typing import Iterable 

import dlt
from dlt.sources.helpers import requests

from common.utils import flatten_dict_by_key
from mappings import MappingDict


#load pipeline functions

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


#Transform pipeline functions

def get_string_id(s :str) -> str:
    """
    Result used to compare text attributes.
    Take several short pieces of string not to work with long string (description)
    Delete everything except letters so as not to depend on different portals' formatting and parsing errors
    """
    if s is None:
        return ""
    else:
        s_part = "".join([
            s[:100], 
            s[500:550], 
            s[1500:1550], 
            s[-300:-250]
        ]).lower()
        return re.sub("[^a-zA-Z]+", "", s_part)

def get_post_id(attr_list :Iterable) -> str:
    attr_parts_str = "".join([get_string_id(s) for s in attr_list])
    return hashlib.sha1(attr_parts_str.encode("UTF-8")).hexdigest()

def prepare_mapping_dict(
    mapping_dict: dict, 
    case_sensitive: bool = False, 
    spaces_sensitive: bool = False
) -> MappingDict:
    
    prepared_dict = {}
    for key, val in mapping_dict.items():
        prepared_key = key
        prepared_val = val
        if not case_sensitive:
            prepared_key = prepared_key.lower()
        if not spaces_sensitive:
            prepared_key = prepared_key.replace(" ", "")
        prepared_dict[prepared_key] = prepared_val
        
    return MappingDict(
        prepared_dict,
        case_sensitive,
        spaces_sensitive,
    )

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