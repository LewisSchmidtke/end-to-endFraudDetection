from pyspark.sql import Row
from pyspark.sql import SparkSession

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
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark