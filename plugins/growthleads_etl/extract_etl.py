import os
import pandas as pd
import re
from typing import Literal
from pathlib import Path
import glob

from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_SOURCES = Literal["routy", "voluum", "manual"]
SCHEMA_NAMES = Literal["bronze", "silver", "gold"]


# utils
def is_csv_available(folder_path: str):
    """
    Checks if there is any .csv file in the folder.

    Args:
        folder_path (str): The path to the folder to monitor.

    Returns:
        bool: True if any .csv file is found, False otherwise.
    """
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            return True
    return False


def standardise_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures all column names in a DataFrame are snake_case"""
    df.columns = [re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower() for col in df.columns]
    return df


def convert_integers_to_floats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise all numerical values as floats"""
    for col in df.select_dtypes(include="int").columns:
        df[col] = df[col].astype(float)
    return df


# readers
def read_data(source: DATA_SOURCES, data_dir: Path):
    """Read data from a source and return a DataFrame with added metadata."""
    print(f"Reading {source} data...")

    source_path = data_dir / source / "*.csv"
    all_files = glob.glob(str(source_path))

    if not all_files:
        raise FileNotFoundError(f"No CSV files found in {source_path}")

    return pd.concat(
        [
            pd.read_csv(file)
            .pipe(standardise_column_names)
            .pipe(convert_integers_to_floats)
            .assign(date=lambda df: pd.to_datetime(df["date"], errors="coerce"))
            .assign(source=source)
            .assign(filename=Path(file).name)
            .assign(ingestion_timestamp=pd.Timestamp.now())
            for file in all_files
        ],
        ignore_index=True,
    )


# db_tools
def load_dataframe_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    schema_name: SCHEMA_NAMES,
    postgres_conn_id: str,
    if_exists: Literal["append", "fail", "replace"] = "append",
):
    """Load a Pandas DataFrame to a PostgreSQL table using pd.to_sql()"""
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists=if_exists,
        index=False,
        method="multi",  # Use multiple-row inserts for efficiency
        chunksize=1000,  # Batch size for inserts
    )
    print(f"Data successfully loaded into '{schema_name}.{table_name}'.")
