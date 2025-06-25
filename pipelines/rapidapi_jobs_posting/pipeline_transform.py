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
import pandas as pd
import datetime as dt
from google.cloud import bigquery

from common.utils import (
    df_to_bq,
    bq_table_to_df,
    bq_merge,
    print_dict,
    google_sheet_to_df
)
from functions import (
    get_post_id,
    LoadsLogger,
)
import mappings
from config import (
    PRINT_SQL,
    JOBS_POSTINGS_FINAL_COLS,
    BQ_DWH_PARAMS,
    BQ_ADB_PARAMS,
    GCP_NAME,
    SERVER, #switching between test/prod parameters
    MAPPING_RULES_EXPORT_URLS,
)

location = BQ_DWH_PARAMS[SERVER]['location']
bq_client = bigquery.Client(location=location)
source_tables_prefix = f"{GCP_NAME[SERVER]}.{BQ_DWH_PARAMS[SERVER]['dataset_name']}."
dataset = BQ_DWH_PARAMS[SERVER]['dataset_name']
analytical_dataset = BQ_ADB_PARAMS[SERVER]['dataset_name']
project = GCP_NAME[SERVER]

pipeline_name = "jobs_posting_transform"

def main():
        
    # fetch new data
    # deal with doubled posts
    # normalize attributes
    # update analytical tables
        
    #----------------------------------------------------fetch new data------------------------------------------------
    
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
    
    new_loads = LoadsLogger(df_posting, dataset, project)
    new_loads.start(pipeline_name)
    df_posting.drop(columns="_dlt_load_id", inplace=True)
    
    
    #----------------------------------------------------deal with doubled posts----------------------------------------
    
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
    
    
    #----------------------------------------------------normalize attributes-------------------------------------------
    
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
    
    #----------------------------------------------------normalize positions--------------------------------------------
    
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
    
    #----------------------------------------------------normalize cities------------------------------------------------
    
    city_clusters_rules = MappingRules(
        "city_clusters", 
        google_sheet_to_df(MAPPING_RULES_EXPORT_URLS["city_clusters"])
    )

    
    df_posting["city_cluster"] = df_posting["city"].map(lambda x: set(("Other")) if pd.isna(x) else city_rules.apply([x]))
    # null -> other, Frankfurt
    df_posting.drop(columns="city", inplace=True)


     #----------------------------------------------------normalize experience requirements------------------------------

    df_posting['years_of_experience']=(df_posting['experience_requirements__months_of_experience']
                                           .map(lambda x: None if pd.isna(x) else ceil(x/12)
                                        )
    )
    df_posting.drop(columns="experience_requirements__months_of_experience", inplace=True)

    #---------------------------------------------------create list of skills--------------------------------------------

    skills_rules = MappingRules(
        "skills", 
        google_sheet_to_df(MAPPING_RULES_EXPORT_URLS["skills"])
    )

    df_posting["skills"] = df_posting["description"].map(lambda x: set() if pd.isna(x) else skills_rules.apply([x]))
       
    skills_set = rules_df.result.unique()
    cloud_skills_sets = dict()
    cloud_skills_sets["Google Cloud Platform"] = {x for x in skills_set if "Google" in x}
    cloud_skills_sets["Microsoft Azure"] = {x for x in skills_set if "Azure" in x}
    cloud_skills_sets["Amazon Web Services"] = {x for x in skills_set if "Amazon" in x}

    for cloud, skills_set in cloud_skills_sets.items():
        df_posting["skills"] = df_posting["skills"].map(
            lambda x: x | {cloud} if x & skills_set else x
        )
        
    df_posting["skills"] = df_posting["skills"].map(
        lambda x: x | {"Cloud"} if x & clouds_set else x
    )

    df_skills = df_posting[["id", "position", "skills"]].explode("skills").dropna()
    
    #----------------------------------------------------update analytical tables-----------------------------------------
    
    #download data to the tmp table
    jobs_columns = list(JOBS_POSTINGS_FINAL_COLS.keys())
    df_posting.rename(columns = {"job_id": "id"}, inplace=True)
    df_posting = df_posting[jobs_columns]
    df_to_bq(df_posting, '_jp_jobs_batch', dataset, project, truncate=True)
    
    # save info about new ids 
    df_dlt_to_post_id.rename(columns = {"_dlt_id": "dlt_id"}, inplace=True)
    df_dlt_to_post_id['matched_at'] = dt.datetime.now()
    df_to_bq(df_dlt_to_post_id, '_jp_dlt_ids_matching', dataset, project, truncate=False)
    
    #update main analytical table
    bq_merge(
        f"{project}.{analytical_dataset}.jobs",
        f"{project}.{dataset}._jp_jobs_batch", 
        "id",
        jobs_columns[1:], #exclude key column
        print_sql = PRINT_SQL,
    )
    
    # log
    new_loads.finish(pipeline_name)

if __name__=="__main__":
    main()
    
