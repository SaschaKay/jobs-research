from typing import Any, Iterable

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
import pandas as pd

from .base.sql_helper import SQLhelper

import logging
logger = logging.getLogger(__name__)

class BQHelper(SQLhelper):
    """
    BigQuery implementation of SQLhelper interface.
    Provides helper methods to interact with BigQuery using pandas and Google Cloud Python client.
    """

    def __init__(
            self, 
            project: str, 
            default_dataset: str = None, 
            client: bigquery.Client = None,
        ):
        self.project = project
        self.default_dataset = default_dataset
        self.client = client or bigquery.Client()

    def full_table_name(self, table: str) -> str:
        """
        Returns full table name in the format `project.dataset.table`.
        If table includes a dataset (i.e. 'dataset.table'), it will be preserved.

        Args:
            table (str): Table name with or without dataset prefix.

        Returns:
            str: Fully qualified table name.
        """
        if "." in table:
            return f"{self.project}.{table}"
        return f"{self.project}.{self.default_dataset}.{table}"

    def table_to_df(self, table: str, condition: str = "") -> pd.DataFrame:
        """
        Read a table from BigQuery to a pandas DataFrame.

        Args:
            table (str): Table name (optionally prefixed with dataset).
            condition (str): Optional WHERE condition (without 'WHERE').

        Returns:
            pd.DataFrame: Resulting data.
        """
        table_full = self.full_table_name(table)
        query = f"SELECT * FROM `{table_full}`"
        if condition:
            query += f" WHERE {condition}"
        return self.client.query(query).to_dataframe()

    def df_to_table(
            self, 
            df: pd.DataFrame, 
            table: str, 
            truncate: bool = False, 
            strict_schema: bool = False,
            schema: bigquery.SchemaField | None = None
    ) -> bigquery.QueryJob:
        """
        Write a pandas DataFrame to a BigQuery table.

        Args:
            df (pd.DataFrame): DataFrame to write.
            table (str): Target table name (optionally prefixed with dataset).
            truncate (bool): If True, table will be deleted before loading.
            strict_schema (bool): If True, passes schema to job config.
            schema (Optional[bigquery.SchemaField]): Schema for the table. If None and strict_schema is True, it will be inferred from the table (if exists).

        Returns:
            BigQuery job.
        """
        table_full = self.full_table_name(table)
        if strict_schema:
            if schema is None:
                # If schema is not provided, try to infer it from the existing table
                try:
                    table_ref = self.client.get_table(table_full)
                    schema = table_ref.schema
                    logger.info(f"Inferred schema for {table_full}: \n" + (",\n".join([f"{f.name}: {f.field_type}" for f in schema])))
                except Exception as e:
                    logger.error(f"Failed to get schema for {table_full}: {e}")
                    raise ValueError(f"Schema must be provided for {table_full} when strict_schema is True.")
            # Create job config with schema
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema
            )
        else:
            if schema is not None:
                logger.warning(
                    f"Schema provided for {table_full} but strict_schema is False. Schema will be ignored."
                )
            # Create job config without schema
            job_config = bigquery.LoadJobConfig(
                write_disposition= bigquery.WriteDisposition.WRITE_APPEND,
            )
        if truncate:
            try:
                self.client.get_table(table_full)
                self.client.query(f"TRUNCATE TABLE `{table_full}`").result()
                logger.info(f"{table_full} was truncated.")
            except NotFound:
                logger.warning(f"{table_full} does not exist, skipping truncate.")

        job = self.client.load_table_from_dataframe(
            df, table_full, job_config
        ) 
        job.result()
        logger.info(f"Loaded {len(df)} rows to {table_full}")
        return job

    def check_duplicates(
        self,
        table: str,
        key_columns: Iterable,
        raise_error: bool = False
    ) -> bool:
        """
        Checks for duplicates in a BigQuery table based on key columns.

        Args:
            table (str): Table name (optionally prefixed with dataset).
            key_columns (Iterable): Unique key columns.
            raise_error (bool): If True, raise error on duplicates.

        Returns:
            bool: True if duplicates exist, else False.
        """
        table_full = self.full_table_name(table)
        group_by_clause = ", ".join(key_columns)

        query = f"""
            SELECT COUNT(1) AS duplicate_count
            FROM (
                SELECT 1
                FROM {table_full}
                GROUP BY {group_by_clause}
                HAVING COUNT(1) > 1
            )
        """

        job = self.client.query(query)
        result  = job.result()
        row = list(result)[0]
        duplicate_count = row["duplicate_count"]

        if duplicate_count > 0:
            message = f"Found {duplicate_count} duplicated values in {table_full} based on key: {key_columns}"
            if raise_error:
                raise ValueError(message)
            else:
                logger.warning(message)
            return True

        return False

    def delete_rows(self, table: str, condition: str) -> bigquery.QueryJob:
        """
        Delete rows from a BigQuery table.

        Args:
            table (str): Table name (optionally prefixed with dataset).
            condition (str): SQL condition (without 'WHERE').

        Returns:
            QueryJob: Result of DELETE query.
        """
        table_full = self.full_table_name(table)
        query = f"DELETE FROM `{table_full}` WHERE {condition}"
        job = self.client.query(query)
        job.result()
        return job

    def merge(
        self,
        destination_table: str,
        source_table: str,
        key_columns: str | Iterable,
        insert_columns: str | Iterable = (),
        update_columns: str | Iterable = (),
        raise_duplicates_error: bool = True,
    ) -> Any:
        """
        Merge source table into destination table using BigQuery SQL MERGE statement.

        Args:
            destination_table (str): Target table to merge into (optionally prefixed with dataset).
            source_table (str): Source table with new data (optionally prefixed with dataset).
            key_columns (str | Iterable): Keys to match on.
            insert_columns (str | Iterable): Columns to insert.
            update_columns (str | Iterable): Columns to update.
            raise_duplicates_error (bool): Whether to raise error on duplicates.

        Returns:
            QueryJob: Result of MERGE query.
        """

        # Check for duplicates before executing merge
        self.check_duplicates(
            source_table, 
            key_columns,
            raise_duplicates_error
        )
        self.check_duplicates(
            destination_table, 
            key_columns,
            raise_duplicates_error
        )

        destination_full = self.full_table_name(destination_table)
        source_full = self.full_table_name(source_table)

        merge_query = self.generate_merge_query(
            destination_table_full_name=destination_full,
            source_table_full_name=source_full,
            key_columns=key_columns,
            insert_columns=insert_columns,
            update_columns=update_columns
        )

        logger.debug(f"Executing query... \n{merge_query}")
        job = self.client.query(merge_query)
        job.result()
        result_info = job._properties.get("statistics").get("query").get("dmlStats")
        logger.info(
            f"Merged {source_full} into {destination_full}"
            f"\nKey columns: self.key_columns"
            f"\nInserted rows: {result_info.get('insertedRowCount')}"
            f"\nUpdated rows: {result_info.get('updatedRowCount')}"
        )

        return job