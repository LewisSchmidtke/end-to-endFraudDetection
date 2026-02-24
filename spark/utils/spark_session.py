from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "FraudDetection") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark