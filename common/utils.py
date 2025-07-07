import os
import time

import json
from copy import deepcopy
from typing import Literal, Iterable, get_args
from sentinels import Sentinel
import warnings

import pandas as pd
import dlt
from dlt.sources.helpers import requests
from google.cloud import storage, bigquery

def check_literal_values(val: str, arg_name: str, literal_type) -> str:
    valid_values = get_args(literal_type)
    if val not in valid_values:
        valid_vals_str = ", ".join(f"'{v}'" for v in valid_values)
        raise ValueError(
            f"Invalid value '{val}' for argument '{arg_name}'. "
            f"Expected one of ({valid_vals_str})."
        )


def print_dict(dict_to_print: dict, header: str = ""):
    print(header)
    for key, val in dict_to_print.items():
        print(f"{key}: {val}")
    print("")


def get_gcp_key():
    GOOGLE_APPLICATION_CREDENTIALS = os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ]  # path to a GCP credential file
    with open(GOOGLE_APPLICATION_CREDENTIALS) as f:
        gcp_key_dict = json.load(f)
    return gcp_key_dict   


def google_sheet_to_df(sheet_url: str) -> str:
    return pd.read_csv(sheet_url.replace("/edit?gid=", "/export?format=csv&gid="))
    

def bq_table_to_df(project, dataset_name, table_ref, bq_client = None):
    if bq_client is None:
        bq_client = bigquery.Client()
    dataset_ref = bigquery.DatasetReference(project, dataset_name)
    table_ref = dataset_ref.table(table_ref)
    table = bq_client.get_table(table_ref)
    return bq_client.list_rows(table).to_dataframe()


def df_to_bq(df, table_name, dataset, project, bq_client=None, truncate=False):
    if bq_client is None:
        bq_client = bigquery.Client()
        
    full_table_name = f"{project}.{dataset}.{table_name}"

    if truncate:
        job = bq_client.query(f"truncate table {full_table_name}")
        job.result()
        print(f"{full_table_name} truncated")
        
    job_config = bigquery.LoadJobConfig(
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if truncate
            else bigquery.WriteDisposition.WRITE_APPEND
        )
    )
    job = bq_client.load_table_from_dataframe(
        df, full_table_name, job_config
    ) 
    job.result()
    print(
        "Loaded {} rows to {}".format(
            len(df), full_table_name
        )
    )


def bytes_to_gcs(content: bytes, gcs_bucket: str, path: str): 
    print(gcs_bucket, path)
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(path)
    blob.upload_from_string(content, content_type="application/octet-stream")
    print(f"Uploaded {path} to GCS")


def flatten_dict_by_key(nested_dict: dict, keys: Iterable):
    """
    Returns a new dict object with selected nested dicts flattened into the top level.

    For each key in `keys`, if it exists in `nested_dict` and its value is a dict,
    it is used to update the top-level dict. The original key is deleted.
    In case of a collision, old keys get rewritten by new.

    This function does not modify the original input.

    Args:
        nested_dict (dict): The input dict object.
        keys (Iterable): Keys whose values are dicts to be flattened.

    Returns:
        dict: A new dict object with flattened structure.
    """
    result = deepcopy(nested_dict)
    for key in keys:
        result.update(nested_dict[key])
        del result[key]
    return result

PaginatedSourceResponseFormat = Literal["json", "parquet"]
@dlt.source
def paginated_source(
    url: str,
    response_format: PaginatedSourceResponseFormat,
    queryparams: dict = None,
    headers: dict = None,
    start_page=1,
    end_page: int = None,
    allow_no_end_page=False,
    delay: int = 1,
    upload_to_gcs: bool = False,
    storage_path: str = None,
    file_name: str = None,
    gcs_bucket: str = None,
):
    """
    Fetches and yields paginated API responses as a DLT resource and optionally uploads the raw responses to Google Cloud Storage (GCS).
    Supports JSON and Parquet response formats and optional delay between requests.

    Args:
        url (str): The base URL of the API endpoint.
        response_format (PaginatedSourceResponseFormat): Format of the API response. Supported: "json", "parquet".
        queryparams (dict, optional): Additional query parameters to include in the API request. Defaults to None.
        headers (dict, optional): HTTP headers to include in the request. Defaults to None.
        start_page (int, optional): The page number to start fetching from. Defaults to 1.
        end_page (int, optional): The last page number to fetch. If None, must set `allow_no_end_page=True`.
        allow_no_end_page (bool, optional): Continues fetching until an empty response. Defaults to False.
        delay (int, optional): Delay in seconds between requests to avoid rate limiting. Defaults to 1.
        upload_to_gcs (bool, optional): Whether to upload raw responses to GCS. Defaults to False.
        storage_path (str, optional): GCS folder path prefix where files will be stored. Required if `upload_to_gcs=True`.
        file_name (str, optional): Base file name for uploaded files. Required if `upload_to_gcs=True`.
        gcs_bucket (str, optional): Name of the target GCS bucket. Required if `upload_to_gcs=True`.

    Yields:
        list or bytes: Parsed data for each page in the format defined by `response_format`.
    """
    @dlt.resource()
    def get_pages():
        
        # Input checks:

        check_literal_values(response_format, "response_format", PaginatedSourceResponseFormat)

        if end_page is None and not allow_no_end_page:
            raise ValueError("Define end_page or set allow_no_end_page=True.")

        if upload_to_gcs:
            if storage_path is None or file_name is None or gcs_bucket is None:
                raise ValueError(
                    "Define file_name, storage_path and gcs_bucket or set upload_to_storage=False."
                )
            else:
                full_path = storage_path + file_name

        if "page" in queryparams:
            raise ValueError(
                "Define page through start_page and end_page parameters, not in queryparams."
            )

        if end_page is not None and start_page > end_page:
            raise ValueError("Start_page can not be greater than end_page.")

        # Fetching data

        page = start_page

        while True:
            # Stop at end_page if defined
            if end_page is not None and page > end_page:
                break

            print(f"Requesting page {page}...")

            # Create a shallow copy of the queryparams dict to avoid mutating the input
            params = dict(queryparams or {})
            params["page"] = page

            response = requests.get(url, headers=headers or {}, params=params)
            if response_format == "parquet":
                data = response.content
            elif response_format == "json":
                data = response.json()["result"]

            print(f"Page {page} was received")

            if not data:
                warnings.warn(f"No data was recieved in a response for page {page}")
                break

            # Upload raw data to file in Google Cloud Storage if required
            if upload_to_gcs:
                print(f"Loading page {page} content to GCS...")
                full_file_path = full_path + f"_{page}.{response_format}"
                bytes_to_gcs(
                    response.content,
                    gcs_bucket=gcs_bucket,
                    path=full_file_path,
                )

            yield data

            page += 1

            # Delay based on API frequency restrictions
            time.sleep(delay)

    return get_pages

def bq_check_duplicates(
    table_full_name: str, 
    key_columns: str|Iterable,
    bq_client=None, 
    raise_error: bool = False
):
    if type(key_columns) == str:
        group_by_clause=key_columns
    else:
        group_by_clause=", ".join(key_columns)

    query=f"""
        select count(1)
        from (
            select 1
            from {table_full_name}
            group by {group_by_clause}
            having count(1)=1
    )"""

    if bq_client is None:
        bq_client = bigquery.Client()
    job = bq_client.query(self._query)
    job.result()



class SqlMergeQuery():
    def __init__(
        self,
        destination_table_full_name: str, 
        source_table_full_name: str, 
        key_columns: str|Iterable,
        insert_columns: str|Iterable = (),
        update_columns: str|Iterable = (),
    ):

        if not insert_columns and not update_columns:
            raise ValueError(
                "At least one of ('insert_columns', 'update_columns') is expected to be not empty."
            )

        self.destination_table_full_name = destination_table_full_name
        self.source_table_full_name = source_table_full_name

        #put all _columns parametrs in lists
        if type(key_columns)==str: 
            self.key_columns=[key_columns]
        else: 
            self.key_columns=list(key_columns)

        if type(insert_columns)==str: 
            self.insert_columns=[insert_columns]
        else:
            self.insert_columns=list(insert_columns)

        if type(update_columns)==str: 
            self.update_columns=[update_columns]
        else:
            self.update_columns=[col for col in update_columns if col not in self.key_columns]

        self._generate_query()

    def _generate_query(self):

        match_clause = "\n    AND ".join(
                [f"t.{col} = s.{col}" for col in self.key_columns]
        )

        if self.update_columns:
            update_clause = ",\n        ".join(
                    [f"t.{col} = s.{col}" for col in self.update_columns]
            )

            update_statement = (
                "\nWHEN MATCHED THEN"
                "\n    UPDATE SET"
                f"\n        {update_clause}"
            )
        else:
            update_statement=""

        if len(self.insert_columns)>0:
            insert_columns_clause = ",\n        ".join(self.insert_columns)
            insert_values_clause = ",\n        ".join([f"s.{col}" for col in self.insert_columns])
            insert_statement = (
                "\nWHEN NOT MATCHED THEN"
                "\n    INSERT("
                f"\n        {insert_columns_clause}"
                "\n    )"
                "\n   VALUES("
                f"\n        {insert_values_clause}"
                "\n    )"
            )
        else: insert_statement=""
        
        self._query = (
            f"MERGE {self.destination_table_full_name} t"
            f"\nUSING {self.source_table_full_name} s"
            f"\n    ON {match_clause}{update_statement}{insert_statement}"
        )

    def print_sql(self):
        print(self._query)

    def execute_bq(self, bq_client=None):
        if bq_client is None:
            bq_client = bigquery.Client()
        job = bq_client.query(self._query)
        job.result()
        return job