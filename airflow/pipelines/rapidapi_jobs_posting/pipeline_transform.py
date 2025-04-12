import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import re

from common.utils import bq_table_to_df

from config import ( 
    COLUMNS_TO_DOWNLOAD,
    BQ_PARAMS,
    GCP_NAME,
    #switching between test/prod parameters
    SERVER
)
from functions import find_position_in_text, collapse_city_groups
from mappings import (prepare_mapping_dict, 
    mapping_dicts_positions, 
    city_clusters)


df_posting = bq_table_to_df(GCP_NAME[SERVER], BQ_PARAMS[SERVER]['dataset_name'], "jobs_posting")

# filtering
df_posting = df_posting[df_posting.locale == "en_DE"]


df_posting = df_posting[
    df_posting.title.map(
        lambda x: not (
            "Student" in x or "Professor" in x or "PhD" in x or "PostDoc" in x
        )
    )
].copy()


#dealing with doubles 
# will be used to compare descriptions
df_posting["description_part"] = df_posting.description.map(
    lambda x: (
        re.sub(
            "[^a-zA-Z]+", "", x[:100] + x[500:550] + x[1500:1550] + x[-300:-250]
        ).lower()
        if x is not None
        else None
    )
)

df_posting = df_posting.groupby(
    by=["title_lower_no_spaces", "company", "city", "description_part"],
    as_index=False,
).first()


# dtypes
df_posting["date_created"] = df_posting["date_created"].dt.floor("D")


# mapping

#preparing fields
df_posting["title_lower_no_spaces"] = df_posting.title.map(
    lambda x: x.lower().replace(" ", "")
)
df_posting["occupation_lower_no_spaces"] = df_posting.occupation.map(
    lambda x: x.lower().replace(" ", "")
)

#preparing mapping fields
map_dicts_positions_prepared = [
    prepare_mapping_dict(*mapping_dict) for mapping_dict in mapping_dicts_positions
]

#normalizing positions

df_posting["new_position"] = None


for md in mapping_dicts_positions:

    if not (md.case_sensitive & md.spaces_sensitive):
        text_columns = ["title_lower_no_spaces", "occupation_lower_no_spaces"]
    elif md.case_sensitive & md.spaces_sensitive:
        text_columns = ["title_lower_no_spaces", "occupation_lower_no_spaces"]
    else:
        raise ValueError(
            "You need a small refinement to use case_sensitive != spaces_sensitive"
        )

    df_posting["new_position"] = df_posting[["new_position", *text_columns]].apply(
        lambda x: (
            x.iloc[0]
            if x.iloc[0] is not None
            else find_position_in_text(x.iloc[1:], md.mapping_dict)
        ),
        axis=1,
    )


df_posting["new_city"] = df_posting.city.map(lambda x: collapse_city_groups(x, city_clusters))

df_posting['years_of_experience']=(df_posting['experience_requirements__months_of_experience']/12)

df_posting = df_posting[COLUMNS_TO_DOWNLOAD]