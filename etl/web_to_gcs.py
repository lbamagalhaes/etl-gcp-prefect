import gzip
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch_data(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean_data(df: pd.DataFrame, color: str = 'yellow') -> pd.DataFrame:
    """Fix dtype issues"""
    if color == 'yellow':
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    if color == 'green':
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    return df

@task()
def write_local_parquet(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def upload_to_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_bucket = GcsBucket.load("etl-gcp-prefect")
    gcs_bucket.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch_data(dataset_url)
    df_cleaned = clean_data(df, color)
    path = write_local_parquet(df_cleaned, color, dataset_file)
    upload_to_gcs(path)

if __name__ == "__main__":
    colors = ["green"]
    years = [2019,2020,2021]
    months = range(1, 13)
    for color in colors: 
        for year in years:
            for month in months:
                try:
                    etl_web_to_gcs(color, year, month)
                except Exception as e:
                    print(e)

