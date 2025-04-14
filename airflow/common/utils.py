import os
import time

import json
from copy import deepcopy
import warnings
from typing import Literal, Iterable, get_args

import dlt
from dlt.sources.helpers import requests
from google.cloud import storage, bigquery


PaginatedSourceResponseFormat = Literal["json", "parquet"]


def print_dict(dict_to_print: dict, header: str = ""):
    print(header)
    for key, val in dict_to_print.items():
        print(f"{key}: {val}")
    print("")



def bq_merge(
    main_table_full_name: str, 
    source_table_full_name: str, 
    key_column: str,
    columns_list: list,
    bq_client=None,
    print_sql: bool = False,
):  
    
    if bq_client is None:
        bq_client = bigquery.Client()
        
    update_clause = ",\n        ".join([f"T.{col} = S.{col}" for col in columns_list])
    columns_list.append(key_column)
    insert_columns = ",\n        ".join(columns_list)
    insert_values = ",\n        ".join([f"S.{col}" for col in columns_list])
    
    query = f"""
    MERGE `{main_table_full_name}` T
    USING `{source_table_full_name}` S
    ON T.{key_column} = S.{key_column}
    
    WHEN MATCHED THEN
    UPDATE SET 
    {update_clause}
    
    WHEN NOT MATCHED THEN
    INSERT (
    {insert_columns}
    )
    VALUES (
    {insert_values}
    )
    """
    
    if print_sql:
        print(query)
    job = bq_client.query(query)
    job.result()

def get_gcp_key():

def get_gcp_key():
    GOOGLE_APPLICATION_CREDENTIALS = os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ]  # path to a GCP credential file

    with open(GOOGLE_APPLICATION_CREDENTIALS) as f:
        gcp_key_dict = json.load(f)

    return gcp_key_dict


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
    @dlt.resource()
    def get_pages():
        # Input checks:

        allowed_response_formats = get_args(PaginatedSourceResponseFormat)

        if response_format not in allowed_response_formats:
            raise ValueError(
                f"Unknown response format: {response_format}. Must be one of: {', '.join(allowed_response_formats)}"
            )

        if end_page is None and not allow_no_end_page:
            raise ValueError("define end_page or set allow_no_end_page=True")

        if upload_to_gcs:
            if storage_path is None or file_name is None or gcs_bucket is None:
                raise ValueError(
                    "define file_name, storage_path and gcs_bucket  or set upload_to_storage = False"
                )
            else:
                full_path = storage_path + file_name

        if "page" in queryparams:
            raise ValueError(
                "define page through start_page and end_page parameters, not in queryparams"
            )

        if end_page is not None and start_page > end_page:
            raise ValueError("start_page can not be greater than end_page")

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
                warnings.warn(f"No data in response for page {page}")
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