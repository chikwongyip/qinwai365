from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import when_matched, when_not_matched, col
from typing import List
import logging


def dynamic_merge_data(
    session: Session,
    source_df: DataFrame,
    table_name: str,
    merge_keys: List[str]
) -> None:
    """
    Dynamically merges a DataFrame into a Snowflake table using specified merge keys.

    Args:
        session: Active Snowpark session
        source_df: DataFrame containing data to merge
        table_name: Fully qualified target table name (e.g., "SCHEMA.TABLE")
        merge_keys: List of column names to use for matching records

    Returns:
        None: Executes merge operation directly on the table

    Raises:
        ValueError: If merge keys are missing from DataFrame or table
    """
    try:
        # Validate merge keys exist in source
        missing_keys = [
            key for key in merge_keys if key not in source_df.columns]
        if missing_keys:
            raise ValueError(
                f"Merge keys missing from source DataFrame: {missing_keys}")

        # Get target table reference
        target_df = session.table(table_name)

        # Validate merge keys exist in target (case-insensitive comparison)
        target_cols = [col.name.upper() for col in target_df.schema.fields]
        missing_target_keys = [
            key for key in merge_keys if key.upper() not in target_cols]
        if missing_target_keys:
            raise ValueError(
                f"Merge keys missing from target table: {missing_target_keys}")

        # Build merge condition as a Snowpark Column expression
        merge_condition = None
        for key in merge_keys:
            condition = (target_df[key] == source_df[key])
            merge_condition = condition if merge_condition is None else merge_condition & condition

        # Build update/insert expressions dynamically
        update_expr = {col: source_df[col]
                       for col in source_df.columns if col not in merge_keys}
        insert_expr = {col: source_df[col] for col in source_df.columns}

        # Validate source columns exist in target for insert
        extra_cols = [col for col in source_df.columns if col.upper()
                      not in target_cols]
        if extra_cols:
            raise ValueError(
                f"Source DataFrame contains columns not in target table: {extra_cols}")

        # Execute merge
        merge_result = target_df.merge(
            source_df,
            merge_condition,
            [
                when_matched().update(update_expr),
                when_not_matched().insert(insert_expr)
            ]
        )

        logging.info(
            f"Successfully merged data into {table_name}: "
            f"{merge_result.rows_updated} rows updated, "
            f"{merge_result.rows_inserted} rows inserted"
        )

    except Exception as e:
        logging.error(f"Merge failed: {str(e)}")
        raise
