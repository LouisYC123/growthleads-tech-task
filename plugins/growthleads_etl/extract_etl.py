import os
import pandas as pd
import re
from pathlib import Path
import glob
from datetime import datetime
import shutil

from airflow.providers.postgres.hooks.postgres import PostgresHook

from growthleads_etl.config import SCHEMA_NAMES, LOAD_TYPES
import hashlib


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


def move_csv_to_archive(source: str, landing_zone: Path, archive_zone: Path):
    """Move CSV files from the landing zone to the archive folder."""

    landing_path = landing_zone / source
    archive_path = archive_zone / source

    os.makedirs(archive_path, exist_ok=True)

    for file in landing_path.glob("*.csv"):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        archive_file = archive_path / f"{file.stem}_{timestamp}{file.suffix}"
        shutil.move(file, archive_file)
        print(f"Moved {file} to {archive_file}")


def standardise_number_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise all numerical values as floats"""
    for col in df.select_dtypes(include="int").columns:
        df[col] = df[col].astype(float)
    return df


def standardise_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures all column names in a DataFrame are snake_case"""
    df.columns = [re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower() for col in df.columns]
    return df


def standardise_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df["date"] = pd.to_datetime(
            df["date"].str.replace("/", "-", regex=False),
            errors="coerce",
            infer_datetime_format=True,
        )
    return df


def standardise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise the DataFrame by applying all standardisation functions."""
    return (
        df.pipe(standardise_column_names)
        .pipe(standardise_dates)
        .pipe(standardise_number_formats)
    )


def add_metadata(df: pd.DataFrame, source: str, filename: str) -> pd.DataFrame:
    """Add metadata columns to the DataFrame."""
    # source_id for event traffic data
    combined = df.astype(str).agg("".join, axis=1)
    df["event_id"] = combined.map(lambda x: hashlib.md5(x.encode()).hexdigest())

    return df.assign(
        source=source,
        filename=filename,
        ingestion_timestamp=pd.Timestamp.now(),
        source_id=hashlib.md5(f"{source}{filename}".encode()).hexdigest(),
    )


# readers
def read_data(source: str, data_dir: Path):
    """Read data from a source and return a DataFrame with added metadata."""
    print(f"Reading {source} data...")

    source_path = data_dir / source / "*.csv"
    all_files = glob.glob(str(source_path))

    if not all_files:
        raise FileNotFoundError(f"No CSV files found in {source_path}")

    df = pd.concat(
        [
            pd.read_csv(file)
            .pipe(standardise_dataframe)
            .pipe(add_metadata, source, Path(file).name)
            for file in all_files
        ],
        ignore_index=True,
    )
    print(f"Read {str(df.shape[0])} rows for '{source}")
    return df


# db_tools
def load_dataframe_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    schema_name: SCHEMA_NAMES,
    postgres_conn_id: str,
    if_exists: LOAD_TYPES,
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
        # method="multi",  # Use multiple-row inserts for efficiency
        # chunksize=1000,  # Batch size for inserts
    )
    print(f"Data successfully loaded into '{schema_name}.{table_name}'.")
