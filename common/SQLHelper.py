import os
import time

from abc import ABC, abstractmethod
from copy import deepcopy
import json
import logging
from typing import Any, get_args, Literal, Iterable

from google.cloud import storage, bigquery
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SQLhelper(ABC):
    """
    Abstract helper class for working with SQL tables.
    Provides an interface for reading, writing, checking, deleting, and merging data.
    Specific database implementations (e.g., BigQuery, Oracle, Postgres) must subclass this.
    """

    @abstractmethod
    def table_to_df(self, table: str, condition: str = "") -> pd.DataFrame:
        """
        Read a SQL table (optionally filtered by condition) into a pandas DataFrame.

        Args:
            table (str): Full table name (may be engine-specific format).
            condition (str): Optional SQL condition to filter rows (without "WHERE").

        Returns:
            pd.DataFrame: Resulting data.
        """
        pass

    @abstractmethod
    def df_to_table(self, df: pd.DataFrame, table: str, condition: str = "", truncate: bool = False) -> Any:
        """
        Write a pandas DataFrame to a SQL table.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            table (str): Target table name.
            condition (str): Optional condition (can be used for upserts, etc).
            truncate (bool): If True, the target table will be truncated before insert.

        Returns:
            Any: Backend-specific result object.
        """
        pass

    @abstractmethod
    def check_duplicates(
        self, 
        table: str, 
        key_columns: Iterable, 
        raise_error: bool = False
    ) -> bool:
        """
        Check if a table contains duplicate rows based on given key columns.

        Args:
            table (str): Table to check.
            key_columns (Iterable): Column(s) to consider as unique key.
            raise_error (bool): If True, raises ValueError on duplicates.

        Returns:
            bool: True if duplicates were found, False otherwise.
        """
        pass

    @abstractmethod
    def delete_rows(
        self,
        table: str,
        condition: str
    ) -> Any:
        """
        Delete rows from a table based on a SQL condition.

        Args:
            table (str): Table name.
            condition (str): SQL WHERE condition without the "WHERE" keyword.

        Returns:
            Any: Backend-specific result object.
        """
        pass

    @abstractmethod
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
        Merge source table into destination table based on key columns.
        This operation is often referred to as UPSERT.

        Args:
            destination_table (str): Table to update.
            source_table (str): Table with new data.
            key_columns (str | Iterable): Column(s) used to match rows between tables.
            insert_columns (str | Iterable): Columns to insert if no match is found.
            update_columns (str | Iterable): Columns to update if a match is found.
            raise_duplicates_error (bool): If True, raises error if duplicates exist in source.

        Returns:
            Any: Backend-specific result object.
        """
        pass

    def generate_merge_query(
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

        #put all _columns parameters in lists
        if type(key_columns) is str: 
            key_columns=[key_columns]
        else: 
            key_columns=list(key_columns)

        if type(insert_columns) is str: 
            insert_columns=[insert_columns]
        else:
            insert_columns=list(insert_columns)

        if type(update_columns) is str: 
            update_columns=[update_columns]
        else:
            update_columns=[col for col in update_columns if col not in key_columns]

        match_clause = "\n    AND ".join(
                [f"t.{col} = s.{col}" for col in key_columns]
        )

        if update_columns:
            update_clause = ",\n        ".join(
                    [f"t.{col} = s.{col}" for col in update_columns]
            )

            update_statement = (
                "\nWHEN MATCHED THEN"
                "\n    UPDATE SET"
                f"\n        {update_clause}"
            )
        else:
            update_statement=""

        if len(insert_columns)>0:
            insert_columns_clause = ",\n        ".join(insert_columns)
            insert_values_clause = ",\n        ".join([f"s.{col}" for col in insert_columns])
            insert_statement = (
                "\nWHEN NOT MATCHED THEN"
                "\n    INSERT("
                f"\n        {insert_columns_clause}"
                "\n    )"
                "\n   VALUES("
                f"\n        {insert_values_clause}"
                "\n    )"
            )
        else: 
            insert_statement=""
        
        query = (
            f"MERGE {destination_table_full_name} t"
            f"\nUSING {source_table_full_name} s"
            f"\n    ON {match_clause}{update_statement}{insert_statement}"
        )

        return query


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

    def df_to_table(self, df: pd.DataFrame, table: str, truncate: bool = False) -> bigquery.QueryJob:
        """
        Write a pandas DataFrame to a BigQuery table.

        Args:
            df (pd.DataFrame): DataFrame to write.
            table (str): Target table name (optionally prefixed with dataset).
            truncate (bool): If True, table will be truncated before loading.

        Returns:
            BigQuery job.
        """
        table_full = self.full_table_name(table)
            
        job_config = bigquery.LoadJobConfig(
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if truncate
                else bigquery.WriteDisposition.WRITE_APPEND
            )
        )
        job = self.client.load_table_from_dataframe(
            df, table_full, job_config
        ) 
        job.result()
        if truncate:
            logger.info(f"{full_table_name} was truncated.")
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
            message = f"Found {duplicate_count} duplicates in {table_full} based on keys: {key_columns}"
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
            self,
            source_table, 
            key_columns,
            raise_duplicates_error
        )
        self.check_duplicates(
            self,
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
            update_columns=update_columns,
            raise_duplicates_error=raise_duplicates_error,
        )

        logger.debug(f"Executing query... \n{self._query}")
        job = self.client.query(merge_query)
        job.result()
        result_info = job._properties.get("statistics").get("query").get("dmlStats")
        logger.info(
            f"Merged {self.source_table_full_name} into {self.destination_table_full_name}"
            f"\nKey columns: self.key_columns"
            f"\nInserted rows: {result_info.get('insertedRowCount')}"
            f"\nUpdated rows: {result_info.get('updatedRowCount')}"
        )

        return job