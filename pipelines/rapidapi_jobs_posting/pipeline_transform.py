import sys
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# adding paths for project modules
CUR_DIR_WARNING = (
    "__file__ was not available, os.getcwd() was used instead. "
    "You may need to change the working directory."
)
try:
    CURRENT_DIRECTORY = os.path.dirname(__file__)
except NameError:
    CURRENT_DIRECTORY = os.getcwd()
    logger.debug(CUR_DIR_WARNING)

if CURRENT_DIRECTORY not in sys.path:
    sys.path.append(CURRENT_DIRECTORY)

from config import PROJECT_ROOT_RELATIVE

PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIRECTORY, PROJECT_ROOT_RELATIVE))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    logger.debug(f"{PROJECT_ROOT} was added to sys.path")

from math import ceil
import pandas as pd
import datetime as dt
from google.cloud import bigquery

from common.utils import SqlMergeQuery, df_to_bq, google_sheet_to_df
from functions import (
    get_post_id,
    LoadsLogger,
)
from mappings import MappingRules, resolve_frankfurt_conflict, link_skills_to_clouds

pipeline_name = "jobs_posting_transform"


def main(**kwargs):
    # fetch new data
    # deal with doubled posts
    # normalize attributes
    # update analytical tables

    # ----------------------------------------------------fetch new data------------------------------------------------
    logger.info(f"Running pipeline {pipeline_name}")
    params = kwargs["params"]
    logger.info(f"Server: {params['server']}")
    bq_dwh_params = params["gcp"]["bq_dwh_params"]
    bq_adb_params = params["gcp"]["bq_adb_params"]
    location = bq_dwh_params["location"]
    project = params["gcp"]["project"]
    bq_client = bigquery.Client(location=location)

    dataset = bq_dwh_params["dataset_name"]
    source_tables_prefix = f"{project}.{dataset}."
    analytical_dataset = bq_adb_params["dataset_name"]

    mapping_rules_urls = params["mapping_rules_urls"]
    jobs_final_cols = params["final_df_cols"]["jobs"]

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
    logger.debug(f"Fetching new data... \n{df_posting_load_query}")
    df_posting = bq_client.query(df_posting_load_query).to_dataframe()
    logger.info(
        f"Fetched {len(df_posting)} raws from `{source_tables_prefix}.jobs_posting`"
    )

    if df_posting.empty:
        logger.warning("⚠️  No data to process")
        sys.exit(0)

    new_loads = LoadsLogger(df_posting, dataset, project)
    new_loads.start(pipeline_name)
    df_posting.drop(columns="_dlt_load_id", inplace=True)

    # ----------------------------------------------------deal with doubled posts----------------------------------------

    # consider posts with the same title, description, location, and hiring company the same
    df_posting["job_id"] = df_posting[
        ["title", "company", "city", "description"]
    ].apply(get_post_id, axis=1, raw=True)

    # marking the last post, only this one will go to the analytical table
    df_posting.sort_values(["job_id", "date_created"], ascending=False, inplace=True)
    df_posting["is_source"] = df_posting.groupby(by="job_id").cumcount() == 0

    # save mapping from old id on new
    df_dlt_to_post_id = df_posting[["_dlt_id", "job_id", "is_source"]].copy(deep=True)

    # get rid of doubles
    df_posting = df_posting[df_posting.is_source].copy()

    df_posting.drop(columns=["_dlt_id", "is_source"], inplace=True)

    # ----------------------------------------------------normalize attributes-------------------------------------------

    # normalize positions

    positions_rules = MappingRules(
        google_sheet_to_df(mapping_rules_urls["positions"]),
        "positions",
    )

    df_posting["positions"] = df_posting[["title", "occupation"]].apply(
        positions_rules.apply, axis=1
    )

    df_positions = (
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
        inplace=True,
    )
    df_posting.drop(columns=["title", "occupation"], inplace=True)

    # normalize cities

    city_clusters_rules = MappingRules(
        google_sheet_to_df(mapping_rules_urls["city_clusters"]),
        "city_clusters",
    )

    df_posting["city_clusters"] = df_posting["city"].map(
        lambda x: (set() if pd.isna(x) else city_clusters_rules.apply([x]))
    )
    df_posting["city_clusters"] = df_posting["city_clusters"].map(
        resolve_frankfurt_conflict
    )

    df_city_clusters = (
        df_posting[["job_id", "city", "city_clusters"]]
        .explode("city_clusters")
        .dropna()
    )
    df_city_clusters.rename(
        columns={"city": "city_raw", "city_clusters": "city_cluster"}, inplace=True
    )
    df_posting.drop(columns="city", inplace=True)

    # normalize experience requirements

    df_posting["years_of_experience"] = df_posting[
        "experience_requirements__months_of_experience"
    ].map(lambda x: None if pd.isna(x) else ceil(x / 12))
    df_posting.drop(
        columns="experience_requirements__months_of_experience", inplace=True
    )

    # create list of skills

    skills_rules = MappingRules(
        google_sheet_to_df(mapping_rules_urls["skills"]), "skills"
    )

    df_posting["skills"] = df_posting["description"].map(
        lambda x: skills_rules.apply([x])
    )

    all_skills = skills_rules.rules_df.result.unique()
    cloud_skills_dict = link_skills_to_clouds(all_skills)

    for cloud, cloud_skills in cloud_skills_dict.items():
        # enrich the set of skills with high-level cloud platform label
        df_posting["skills"] = df_posting["skills"].map(
            lambda all_skills: all_skills | {cloud}
            if (all_skills & cloud_skills)
            else all_skills
        )

    clouds_set = set(cloud_skills_dict.keys())
    # enrich the set of skills with high-level label 'Cloud'
    df_posting["skills"] = df_posting["skills"].map(
        lambda all_skills: all_skills | {"Cloud"}
        if all_skills & clouds_set
        else all_skills
    )

    df_posting["skills"] = df_posting["skills"].map(
        list
    )  # for downloading in Big Query
    df_skills = df_posting[["job_id", "skills"]].explode("skills").dropna()
    df_skills.rename(columns={"skills": "skill"}, inplace=True)

    # ----------------------------------------------------update analytical tables-----------------------------------------

    # download data to the tmp table
    df_posting.rename(columns={"job_id": "id"}, inplace=True)
    df_posting = df_posting[jobs_final_cols]
    df_to_bq(df_posting, "_jp_jobs_batch", dataset, project, truncate=True)
    df_to_bq(df_skills, "_jp_jobs_skills_batch", dataset, project, truncate=True)
    df_to_bq(df_positions, "_jp_jobs_positions_batch", dataset, project, truncate=True)
    df_to_bq(
        df_city_clusters,
        "_jp_jobs_city_clusters_batch",
        dataset,
        project,
        truncate=True,
    )

    # save info about new ids
    df_dlt_to_post_id.rename(columns={"_dlt_id": "dlt_id"}, inplace=True)
    df_dlt_to_post_id["matched_at"] = dt.datetime.now()
    df_to_bq(
        df_dlt_to_post_id, "_jp_dlt_ids_matching", dataset, project, truncate=False
    )

    # update main analytical table
    SqlMergeQuery(
        f"{project}.{analytical_dataset}.jobs",
        f"{project}.{dataset}._jp_jobs_batch",
        "id",
        jobs_final_cols,
        jobs_final_cols,
    ).execute_bq()

    # log
    new_loads.finish(pipeline_name)


if __name__ == "__main__":
    main()