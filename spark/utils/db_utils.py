import os
from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent.parent / "credentials.env"
load_dotenv(dotenv_path=env_path)

# POSTGRES_HOST for local mode and POSTGRES_HOST_DOCKER to run in docker
JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

JDBC_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Reads a full table from postgres into a spark df via JDBC

    Args:
        spark (SparkSession): Active spark session used to create the df
        table_name (str): Name of the postgres table to read
    Returns:
        DataFrame: Spark df containing the full table.
    """
    return spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPERTIES)


def write_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Writes a spark df to a postgres table via JDBC.

    Args:
        df (DataFrame): Spark df to write
        table_name (str): Name of the target postgres table
        mode (str): Write mode, defaults to "overwrite"
    Returns:
        None
    """
    df.write.jdbc(url=JDBC_URL, table=table_name, mode=mode, properties=JDBC_PROPERTIES)