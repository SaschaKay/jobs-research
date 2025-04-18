import warnings
import sys
import os

# adding paths for project modules
CUR_DIR_WARNING = (
    "__file__ was not available, os.getcwd() was used instead. "
    "You may need to change the working directory."
)
try:
    CURRENT_DIRECTORY = os.path.dirname(__file__)
except NameError:
    CURRENT_DIRECTORY = os.getcwd()
    warnings.warn(CUR_DIR_WARNING)

if CURRENT_DIRECTORY not in sys.path:
    sys.path.append(CURRENT_DIRECTORY)
    
from config import PROJECT_ROOT_RELATIVE
PROJECT_ROOT = os.path.abspath(
    os.path.join(CURRENT_DIRECTORY, PROJECT_ROOT_RELATIVE)
)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    print(f"{PROJECT_ROOT} was added to sys.path")
    
from math import ceil
import datetime as dt
import pandas as pd
from google.cloud import bigquery

from common.utils import (
    df_to_bq,
    bq_merge,
)
from functions import (
    find_position_in_text, 
    collapse_city_groups, 
    prepare_mapping_dict,
    get_post_id,
)
from mappings import (
    POSITIONS, 
    CITY_CLUSTERS,
)
from config import (
    PRINT_SQL,
    JOBS_POSTINGS_FINAL_COLS,
    BQ_PARAMS,
    GCP_NAME,
    SERVER, #switching between test/prod parameters
)

bq_client = bigquery.Client()
source_tables_prefix = f"{GCP_NAME[SERVER]}.{BQ_PARAMS[SERVER]['dataset_name']}."
dataset = BQ_PARAMS[SERVER]['dataset_name']
project = GCP_NAME[SERVER]
pipeline_name = "jobs_posting_transform"



df_posting_load_query = f"""
with processed_loads as (
  select dlt_load_id
  from `{source_tables_prefix}_jp_processed_loads`
  where processed_by = '{pipeline_name}'
  group by dlt_load_id
  having max(finished_at) is not Null
)

, new_loads as (
  select distinct load_id
  from `{source_tables_prefix}._dlt_loads` as dl
  left join processed_loads as pl on dl.load_id = pl.dlt_load_id
  where dl.status = 0
    and pl.dlt_load_id is Null
)

select
    _dlt_load_id
    ,_dlt_id
    ,company
    ,city
    ,title
    ,occupation
    ,url
    ,portal
    ,experience_requirements__months_of_experience
    ,date_created
    ,description 
from `{source_tables_prefix}.jobs_posting` as jp
inner join new_loads nl on jp._dlt_load_id = nl.load_id
where locale = "en_DE"
"""
if PRINT_SQL:
    print(df_posting_load_query)
df_posting = bq_client.query(df_posting_load_query).to_dataframe()
print(f"Fetched {len(df_posting)} raws from `{source_tables_prefix}.jobs_posting`")

if df_posting.empty:
    print("No data to process")
    sys.exit(0)

df_new_loads = pd.DataFrame(df_posting["_dlt_load_id"].drop_duplicates()).copy(deep = True)
df_new_loads.rename(columns={"_dlt_load_id":"dlt_load_id"}, inplace=True)
df_new_loads["processed_by"] = pipeline_name
df_new_loads["started_at"] = dt.datetime.now()
df_to_bq(df_new_loads, '_jp_processed_loads', dataset, project, truncate=False)

df_posting.drop(columns="_dlt_load_id", inplace=True)

#deal with doubled posts
#consider posts with the same title, description, location, and hiring company the same

df_posting["job_id"] = df_posting[["title", "company", "city", "description"]].apply(get_post_id, axis=1, raw=True)

#marking the last post, only this one will go to the analytical table
df_posting.sort_values(
    ["job_id", "date_created"], 
    ascending = False, 
    inplace = True
)
df_posting["is_source"] = df_posting.groupby(by="job_id").cumcount()==0

#save mapping from old id on new
df_dlt_to_post_id = df_posting[["_dlt_id", "job_id", "is_source"]].copy(deep = True)

#get rid of doubles
df_posting = df_posting[df_posting.is_source].copy()

df_posting.drop(columns=['_dlt_id', 'is_source'], inplace = True)

#preparing fields for mapping attributes
df_posting["title_lower_no_spaces"] = df_posting.title.map(
    lambda x: x.lower().replace(" ", "")
)
df_posting["occupation_lower_no_spaces"] = df_posting.occupation.map(
    lambda x: x.lower().replace(" ", "")
)

#preparing mapping rules
map_dicts_positions_prepared = [
    prepare_mapping_dict(*mapping_dict) for mapping_dict in POSITIONS
]

#normalizing positions
df_posting["position"] = None

for md in map_dicts_positions_prepared:

    if not (md.case_sensitive & md.spaces_sensitive):
        text_columns = ["title_lower_no_spaces", "occupation_lower_no_spaces"]
    elif md.case_sensitive & md.spaces_sensitive:
        text_columns = ["title", "occupation"]
    else:
        raise ValueError(
            "You need a small refinement to use case_sensitive != spaces_sensitive"
        )

    df_posting["position"] = df_posting[["position", *text_columns]].apply(
        lambda x: (
            x.iloc[0]
            if x.iloc[0] is not None
            else find_position_in_text(x.iloc[1:], md.mapping_dict)
        ),
        axis=1,
    )
    
df_posting.drop(columns=[
    "title_lower_no_spaces",
    "occupation_lower_no_spaces",
    "title",
    "occupation"
], inplace=True)

#normalizing cities
df_posting["city_group"] = df_posting.city.map(lambda x: collapse_city_groups(x, CITY_CLUSTERS))
df_posting.drop(columns="city", inplace=True)

#normalizing cities
df_posting['years_of_experience']=(df_posting['experience_requirements__months_of_experience']
                                       .map(lambda x: None if pd.isna(x) else ceil(x/12)
                                    )
)
df_posting.drop(columns="experience_requirements__months_of_experience", inplace=True)

#downloading data
jobs_columns = list(JOBS_POSTINGS_FINAL_COLS.keys())
df_posting.rename(columns = {"job_id": "id"}, inplace=True)
df_posting = df_posting[jobs_columns]
df_to_bq(df_posting, '_jp_jobs_batch', dataset, project, truncate=True)

df_dlt_to_post_id.rename(columns = {"_dlt_id": "dlt_id"}, inplace=True)
df_dlt_to_post_id['matched_at'] = dt.datetime.now()
df_to_bq(df_dlt_to_post_id, '_jp_dlt_ids_matching', dataset, project, truncate=False)

#updating tables

bq_merge(
    f"{project}.jp.jobs",
    f"{project}.{dataset}._jp_jobs_batch", 
    "id",
    jobs_columns[1:], #exclude key column
    print_sql = PRINT_SQL,
)

df_new_loads["finished_at"] = dt.datetime.now()
df_to_bq(df_new_loads, '_jp_processed_loads', dataset, project, truncate=False)
