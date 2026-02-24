import os
from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent.parent / "credentials.env"
load_dotenv(dotenv_path=env_path)

JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST_DOCKER')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

JDBC_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPERTIES)


def write_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    df.write.jdbc(url=JDBC_URL, table=table_name, mode=mode, properties=JDBC_PROPERTIES)