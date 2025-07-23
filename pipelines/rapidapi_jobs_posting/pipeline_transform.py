import sys

from math import ceil
import pandas as pd
import datetime as dt
from google.cloud import bigquery

from common.utils import google_sheet_to_df
from common.bq_helper import BQHelper
from functions import (
    get_post_id,
    LoadsLogger,
)
from mappings import(
    MappingRules,
    resolve_frankfurt_conflict,
    link_skills_to_clouds
)
from config import (
    JOBS_POSTINGS_FINAL_COLS,
    BQ_DWH_PARAMS,
    BQ_ADB_PARAMS,
    GCP_NAME,
    SERVER, #switching between test/prod parameters
    MAPPING_RULES_EXPORT_URLS,
)

import logging
from common.logging_config import setup_logging
logger = logging.getLogger(__name__)


location = BQ_DWH_PARAMS[SERVER]['location']
bq_client = bigquery.Client(location=location)
source_tables_prefix = f"{GCP_NAME[SERVER]}.{BQ_DWH_PARAMS[SERVER]['dataset_name']}."
dataset = BQ_DWH_PARAMS[SERVER]['dataset_name']
analytical_dataset = BQ_ADB_PARAMS[SERVER]['dataset_name']
project = GCP_NAME[SERVER]

pipeline_name = "jobs_posting_transform"
logger.info(f"Running pipeline {pipeline_name}")

def main():
        
    # fetch new data
    # deal with doubled posts
    # normalize attributes
    # update analytical tables
        
    #----------------------------------------------------fetch new data------------------------------------------------
    
    df_posting_load_query = f"""
    with processed_loads as (
      select dlt_load_id
      from `{source_tables_prefix}._jp_processed_loads`
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
    logger.info(f"Fetching new data... \n{df_posting_load_query}")
    df_posting = bq_client.query(df_posting_load_query).to_dataframe()
    logger.info(f"Fetched {len(df_posting)} raws from `{source_tables_prefix}.jobs_posting`")
    
    if df_posting.empty:
        logger.warning("⚠️  No data to process")
        sys.exit(0)
    new_loads = LoadsLogger(df_posting, pipeline_name, dataset, project)
    new_loads.start()
    df_posting.drop(columns="_dlt_load_id", inplace=True)
    df_posting['date_created'] = df_posting['date_created'].dt.date
    
    
    #----------------------------------------------------deal with doubled posts----------------------------------------
    
    #consider posts with the same title, description, location, and hiring company the same
    df_posting["job_id"] = df_posting[["title", "company", "city", "description"]].apply(get_post_id, axis=1, raw=True)
    
    #marking the last post, only this one will go to the analytical table
    df_posting.sort_values(
        ["job_id", "date_created"], 
        ascending=False, 
        inplace=True
    )
    df_posting["is_source"] = df_posting.groupby(by="job_id").cumcount()==0
    
    #save mapping from old id on new
    df_dlt_to_post_id = df_posting[["_dlt_id", "job_id", "is_source"]].copy(deep = True)
    
    #get rid of doubles
    df_posting = df_posting[df_posting.is_source].copy()
    
    df_posting.drop(columns=['_dlt_id', 'is_source'], inplace = True)
    
    
    #----------------------------------------------------normalize attributes-------------------------------------------
    
    #normalize positions

    positions_rules = MappingRules(
        google_sheet_to_df(
            MAPPING_RULES_EXPORT_URLS["positions"]),
            "positions",
    )
    
    df_posting["positions"] = df_posting[["title", "occupation"]].apply(
        positions_rules.apply, axis=1
    )

    df_positions=(
        df_posting[["job_id", "title", "occupation", "positions"]]
            .explode("positions")
            .dropna()
    )
    df_positions.rename(
        columns={
            "title": "title_raw", 
            "occupation": "occupation_raw",
            "positions": "position",
            },
        inplace=True
    )
    df_posting.drop(
        columns=["title","occupation"], 
        inplace=True
    )
    
    #normalize citie
    
    city_clusters_rules = MappingRules(
        google_sheet_to_df(MAPPING_RULES_EXPORT_URLS["city_clusters"]),
        "city_clusters",
    )

    df_posting["city_clusters"] = df_posting["city"].map(
        lambda x: (set() if pd.isna(x) else city_clusters_rules.apply([x]))
    )
    df_posting["city_clusters"] = df_posting["city_clusters"].map(resolve_frankfurt_conflict)
    
    df_city_clusters=(
        df_posting[["job_id", "city", "city_clusters"]]
            .explode("city_clusters")
            .dropna()
    )
    df_city_clusters.rename(
        columns={
            "city": "city_raw", 
            "city_clusters": "city_cluster"
        }, 
        inplace=True)
    df_posting.drop(columns = "city", inplace=True)

    #normalize experience requirements

    df_posting['years_of_experience']=(
        df_posting['experience_requirements__months_of_experience']
            .map(lambda x: None if pd.isna(x) else ceil(x/12))
    )
    df_posting.drop(columns="experience_requirements__months_of_experience", inplace=True)

    #create list of skills

    skills_rules = MappingRules(
        google_sheet_to_df(MAPPING_RULES_EXPORT_URLS["skills"]), "skills"
    )

    df_posting["skills"] = df_posting["description"].map(lambda x: skills_rules.apply([x]))

    all_skills = skills_rules.rules_df.result.unique()
    cloud_skills_dict = link_skills_to_clouds(all_skills)


    for cloud, cloud_skills in cloud_skills_dict.items():
    # enrich the set of skills with high-level cloud platform label
        df_posting["skills"] = df_posting["skills"].map(
            lambda all_skills: 
                all_skills | {cloud} 
                    if (all_skills & cloud_skills) 
                        else all_skills
        )

    clouds_set = set(cloud_skills_dict.keys())
    # enrich the set of skills with high-level label 'Cloud'
    df_posting["skills"] = df_posting["skills"].map(
        lambda all_skills:
             all_skills | {"Cloud"} 
                if all_skills & clouds_set 
                    else all_skills
    )

    df_posting["skills"] = df_posting["skills"].map(list) #for downloading in Big Query
    df_skills = (
        df_posting[["job_id", "skills"]]
            .explode("skills")
            .dropna()
    )
    df_skills.rename(columns={"skills": "skill"}, inplace=True)

    #----------------------------------------------------update analytical tables-----------------------------------------
    
    #download data to the tmp table
    bq = BQHelper(project=project,default_dataset=dataset)
    df_posting.rename(columns = {"job_id": "id"}, inplace=True)
    df_posting = df_posting[JOBS_POSTINGS_FINAL_COLS]
    bq.df_to_table(df_posting, '_jp_jobs_batch', truncate=True, strict_schema=True)
    bq.df_to_table(df_skills, "_jp_jobs_skills_batch", truncate=True)
    bq.df_to_table(df_positions, "_jp_jobs_positions_batch", truncate=True)
    bq.df_to_table(df_city_clusters, "_jp_jobs_city_clusters_batch", truncate=True)
    
    #save info about new ids 
    df_dlt_to_post_id.rename(columns = {"_dlt_id": "dlt_id"}, inplace=True)
    df_dlt_to_post_id['matched_at'] = dt.datetime.now()
    bq.df_to_table(df_dlt_to_post_id, '_jp_dlt_ids_matching', truncate=False)
    
    #update main analytical table
    bq.merge(
        destination_table=f"{analytical_dataset}.jobs",
        source_table=f"{dataset}._jp_jobs_batch",  
        key_columns=["id"],
        insert_columns=JOBS_POSTINGS_FINAL_COLS,
        update_columns=JOBS_POSTINGS_FINAL_COLS,
        raise_duplicates_error=True
    )
    
    # log
    new_loads.finish()

if __name__=="__main__":
    setup_logging()
    main()