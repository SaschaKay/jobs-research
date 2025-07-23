from abc import ABC, abstractmethod
from typing import Any, Iterable

import pandas as pd

import logging
logger = logging.getLogger(__name__)


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
    def df_to_table(self, df: pd.DataFrame, table: str, truncate: bool = False) -> Any:
        """
        Write a pandas DataFrame to a SQL table.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            table (str): Target table name.
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
    
    @staticmethod
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