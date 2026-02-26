from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def compute_device_features(df: DataFrame, payment_methods_df: DataFrame) -> DataFrame:
    """
    Computes device and payment method based features for each transaction. These features
    capture suspicious device behavior and payment method patterns.

    Features computed:
        - device_unique_users_24h: Number of distinct users using this device in the last 24 hours.
          A device used by multiple users is suspicious and may indicate account takeover.
        - device_unique_payment_methods_24h: Number of distinct payment methods used on this device
          in the last 24 hours. High number suggests card probing or card cracking.
        - payment_method_age_days: Age of the payment method in days at the time of the transaction.
          Newly added payment methods used immediately are a strong account takeover signal.
        - is_new_payment_method: 1 if the payment method is less than 1 day old at transaction time,
          0 otherwise.

    Args:
        df (DataFrame): Raw transactions DataFrame. Expected columns: device_id, user_id, payment_id, transaction_timestamp
        payment_methods_df (DataFrame): Payment methods reference DataFrame. Expected columns: payment_method_id, created_at
    Returns:
        DataFrame: Original DataFrame with device feature columns appended.
    """
    # We convert the timestamp to unix seconds so we can use rangeBetween (uses numeric offsets)
    df = df.withColumn("ts_unix", F.unix_timestamp("transaction_timestamp"))

    # Join payment method creation timestamp onto transactions
    df = df.join(
        payment_methods_df.select(F.col("payment_method_id"),F.unix_timestamp("created_at").alias("payment_created_unix")),
        on=df["payment_id"] == payment_methods_df["payment_method_id"], how="left"
    ).drop("payment_method_id")

    ######################################### DEVICE 24 hour WINDOW ################################################
    device_24h_window = (Window.partitionBy("device_id").orderBy("ts_unix").rangeBetween(-86400, 0))

    # Number of distinct users on this device in last 24h -> shared device between users can be suspicious
    df = df.withColumn(
        "device_unique_users_24h",
        F.approx_count_distinct("user_id").over(device_24h_window)
    )

    # Number of distinct payment methods used on this device in last 24h | Different to velocity features unique payments,
    # this one checks payment methods from one device
    df = df.withColumn(
        "device_unique_payment_methods_24h",
        F.approx_count_distinct("payment_id").over(device_24h_window)
    )

    ######################################### PAYMENT METHOD AGE ################################################
    # Age of payment method (in days) at time of transaction
    df = df.withColumn(
        "payment_method_age_days",
        (F.col("ts_unix") - F.col("payment_created_unix")) / 86400
    )

    # Binary flag for payment methods less than 1 day old
    df = df.withColumn(
        "is_new_payment_method",
        F.when(F.col("payment_method_age_days") < 1, 1).otherwise(0).cast("integer")
    )

    df = df.drop("ts_unix", "payment_created_unix") # Drop helper col

    return df