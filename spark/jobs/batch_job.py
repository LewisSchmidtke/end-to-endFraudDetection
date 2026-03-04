from pathlib import Path
from pyspark.sql import SparkSession

from spark.utils.spark_utils import create_spark_session
from spark.utils.db_utils import read_table
from spark.features.velocity_features import compute_velocity_features
from spark.features.amount_features import compute_amount_features
from spark.features.behavioral_features import compute_behavioral_features
from spark.features.device_features import compute_device_features

import src.constants as const


ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_PATH = ROOT / const.FEATURE_PATH


def run_batch(spark_sess: SparkSession) -> None:
    """
    Reads raw transaction data from database, computes all features, and writes the feature table to parquet.

    Args:
        spark_sess (SparkSession): Active SparkSession
    """
    # Read from db write to parquet
    transactions_df = read_table(spark_sess, "transactions")
    merchants_df = read_table(spark_sess, "merchants")
    payment_methods_df = read_table(spark_sess, "payment_methods")

    df = compute_velocity_features(transactions_df)
    df = compute_amount_features(df)
    df = compute_behavioral_features(df, merchants_df)
    df = compute_device_features(df, payment_methods_df)

    df.write.mode("overwrite").parquet(str(OUTPUT_PATH))


if __name__ == "__main__":
    spark = create_spark_session(app_name="FraudDetection_FeatureEngineering")
    run_batch(spark)
    spark.stop()