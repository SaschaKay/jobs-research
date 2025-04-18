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

import datetime as dt
from io import BytesIO
import hashlib
from math import ceil
import pandas as pd
import pyarrow.parquet as pq
import re
from typing import Iterable 

import dlt
from dlt.sources.helpers import requests

from common.utils import (
    df_to_bq,
    flatten_dict_by_key,
    bq_merge,
)
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



class LoadsLogger():
    
    def __init__(self, df_posting, dataset, project):
        self.dataset = dataset
        self.project = project
        self.df_new_loads = pd.DataFrame(df_posting["_dlt_load_id"].drop_duplicates()).copy(deep = True)
        self.df_new_loads.rename(columns={"_dlt_load_id":"dlt_load_id"}, inplace=True)

    def get_df(self):
        return self.df_new_loads

    def start(self, pipeline_name):
        self.df_new_loads["processed_by"] = pipeline_name
        self.df_new_loads["started_at"] = dt.datetime.now()
        df_to_bq(self.df_new_loads, '_jp_processed_loads', self.dataset, self.project, truncate=False)

    def finish(self, pipeline_name):
        self.df_new_loads["processed_by"] = pipeline_name
        self.df_new_loads["finished_at"] = dt.datetime.now()
        df_to_bq(df_new_loads, '_jp_processed_loads', self.dataset, self.project, truncate=False)
            