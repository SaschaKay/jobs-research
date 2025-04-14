import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import re
import pandas as pd

from common.utils import df_to_bq

from config import ( 
    JOBS_POSTINGS_FINAL_COLS,
    BQ_PARAMS,
    GCP_NAME,
    #switching between test/prod parameters
    SERVER
)
from functions import find_position_in_text, collapse_city_groups
from mappings import (prepare_mapping_dict, 
    mapping_dicts_positions, 
    city_clusters)


def main():

#load semiraw data from BigQuery
df_posting_query = f"""
SELECT
    _dlt_id
    ,company
    ,city
    ,title
    ,occupation
    ,url
    ,portal
    ,experience_requirements__months_of_experience
    ,date_created
    ,locale
    ,description
FROM `{GCP_NAME[SERVER]}.{BQ_PARAMS[SERVER]['dataset_name']}.jobs_posting`
WHERE 
    locale = "en_DE"
"""
df_posting = bq_client.query(query).to_dataframe()


#filter not relevant data
df_posting = df_posting[
    df_posting.title.map(
        lambda x: not (
            "Student" in x or "Professor" in x or "PhD" in x or "PostDoc" in x
        )
    )
].copy()


#deal with doubled posts
df_posting["description_part"] = df_posting.description.map(
    lambda x: (
        re.sub(
            #will be used to compare descriptions
            "[^a-zA-Z]+", "", #delete everything except letters to not depend on different portals formatting
            (x[:100] + x[500:550] + x[1500:1550] + x[-300:-250]) #take several short pieces to not work with a whole field
        ).lower()
        if x is not None
        else None
    )
)
#consider posts with the same title, descriplion, location and hireing company the same
df_posting = df_posting.groupby(
    by=["title_lower_no_spaces", "company", "city", "description_part"],
    as_index=False,
).first()
#clean 
del df_posting.title_lower_no_spaces
del df_posting.description_part

#trunc date
df_posting["date_created"] = df_posting["date_created"].dt.floor("D")


#mapping

#preparing fields
df_posting["title_lower_no_spaces"] = df_posting.title.map(
    lambda x: x.lower().replace(" ", "")
)
df_posting["occupation_lower_no_spaces"] = df_posting.occupation.map(
    lambda x: x.lower().replace(" ", "")
)

#preparing mapping rules
map_dicts_positions_prepared = [
    prepare_mapping_dict(*mapping_dict) for mapping_dict in mapping_dicts_positions
]

#normalizing positions
df_posting["position"] = None

for md in mapping_dicts_positions:

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
#delete old fields
df_posting.drop(columns=[
    "title_lower_no_spaces",
    "occupation_lower_no_spaces",
    "title",
    "occupation"
], inplace=True)

df_posting["city_group"] = df_posting.city.map(lambda x: collapse_city_groups(x, city_clusters))
del df_posting.city

df_posting['years_of_experience']=(df_posting['experience_requirements__months_of_experience']/12)
del df_posting['experience_requirements__months_of_experience']

df_posting = df_posting[JOBS_POSTINGS_FINAL_COLS]

df_to_bq(df_posting, project, dataset_name, table_name, tuncate=False):

if __name__=="__main__":
    main()