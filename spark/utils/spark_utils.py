import sys
import os
from datetime import datetime
from decimal import Decimal
from pyspark.sql import Row
from pyspark.sql import SparkSession


def _configure_windows_env() -> None:
    """
    Sets HADOOP_HOME, JAVA_HOME and updates PATH for Windows so PySpark can find winutils.exe and the JDK without
    requiring manual env-var setup each session. No-ops on non-Windows platforms.

    Returns:
        None
    """
    if sys.platform != "win32":
        return

    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    java_home = os.environ.get("JAVA_HOME", r"C:\Program Files\Zulu\zulu-17")

    path_entries = os.environ.get("PATH", "").split(os.pathsep)
    for entry in [os.path.join(java_home, "bin"), os.path.join(hadoop_home, "bin")]:
        if entry not in path_entries:
            os.environ["PATH"] = entry + os.pathsep + os.environ["PATH"]

def convert_dicts_to_spark_rows(rows: list[dict]) -> list[Row]:
    """
    Converts a list of dictionaries to a list of Spark Row objects for use with SparkSession.createDataFrame.

    Args:
        rows (list[dict]): List of dictionaries to convert.
    Returns:
        list[Row]: List of Spark Row objects.
    """
    return [Row(**row) for row in rows]


def create_spark_session(app_name: str = "FraudDetection", master: str = "local[*]") -> SparkSession:
    """
    Create a SparkSession object.

    Args:
        app_name (str): Name of the session.
        master (str): Spark master URL. Use 'local[*]' for local development (default) or 'spark://spark-master:7077' when running against the Docker Spark cluster
    Returns:
        SparkSession: The created SparkSession object.
    """
    _configure_windows_env()

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def filter_single_transaction(transaction: dict):
    """
    Filters a single transaction message so that it matches the data from postgres

    Args:
        transaction (dict): Transaction message to filter
    Returns:
        dict: Filtered transaction
    """
    transaction_filtered = {
        "user_id": transaction["user_id"],
        "device_id": transaction["device_id"],
        "transaction_amount_usd": Decimal(str(round(transaction["transaction_amount_usd"], 2))),
        "transaction_status": transaction["transaction_status"],
        "payment_id": transaction["payment_id"],
        "transaction_timestamp": datetime.fromisoformat(str(transaction["transaction_timestamp"])),
        "transaction_country": transaction["transaction_country"],
        "merchant_id": transaction["merchant_id"],
        "transaction_channel": transaction["transaction_channel"],
        "merchant_category": transaction["merchant_category"],
    }

    return transaction_filtered