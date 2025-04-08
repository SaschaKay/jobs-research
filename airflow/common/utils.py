import os
import time

import json
from copy import deepcopy
import warnings
from typing import Literal, Iterable, get_args

import dlt
from dlt.sources.helpers import requests
from google.cloud import storage


PaginatedSourceResponseFormat = Literal["json", "parquet"]


def get_gcp_key():

    GOOGLE_APPLICATION_CREDENTIALS = os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ]  # path to a GCP credential file

    with open(GOOGLE_APPLICATION_CREDENTIALS) as f:
        gcp_key_dict = json.load(f)

    return gcp_key_dict


def upload_bytes_to_gcs(content: bytes, gcs_bucket: str, path: str):
    
    print(gcs_bucket, path)
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(path)
    blob.upload_from_string(content, content_type="application/octet-stream")

    print(f"Uploaded {path} to GCS")


def flatten_dict_by_key(nested_dict: dict, keys: Iterable):
    """
    Returns a new dict object with selected nested dicts flattened into the top level.

    For each key in `keys`, if the key exists in `nested_dict` and its value is a dict,
    it used to update the top-level dict. The original key is deleted.
    In case of collision old keys get rewriten by new.

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

            print(f"Page {page} was recieved")

            if not data:
                warnings.warn(f"No data in response for page {page}")
                break

            # Upload raw data to file in Google Cloud Storage if required
            if upload_to_gcs:
                print(f"Loading page {page} content to GCS...")
                full_file_path = full_path + f"_{page}.{response_format}"
                upload_bytes_to_gcs(
                    response.content,
                    gcs_bucket=gcs_bucket,
                    path=full_file_path,
                )

            yield data

            page += 1

            # Delay based on API frequency restrictions
            time.sleep(delay)

    return get_pages

