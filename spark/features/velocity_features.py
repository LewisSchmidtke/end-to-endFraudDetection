from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def compute_velocity_features(df: DataFrame) -> DataFrame:
    """
    Computes velocity features for each transaction. Velocity features capture
    how much activity a user or device has had in recent time windows.

    Features computed:
        User velocity (1h):
            - user_transaction_count_1h: Number of transactions in the last hour
            - user_decline_count_1h: Number of declined transactions in the last hour
            - user_decline_rate_1h: Decline rate in the last hour
            - user_unique_payment_methods_1h: Number of unique payment methods used in the last hour
        User velocity (5min):
            - user_transaction_count_5min: Number of transactions in the last 5 minutes
        Device velocity (5min):
            - device_transaction_count_5min: Number of transactions from this device in the last 5 minutes

    Args:
        df (DataFrame): Raw transactions DataFrame. Expected columns: user_id, device_id, payment_id, transaction_status, transaction_timestamp
    Returns:
        DataFrame: Original DataFrame with velocity feature columns appended.
    """
    # We convert the timestamp to unix seconds so we can use rangeBetween (uses numeric offsets)
    df = df.withColumn("ts_unix", F.unix_timestamp("transaction_timestamp"))

    ######################################### USER 1 hour WINDOW ################################################
    # We look at all transactions by the same user, 1h prior to the one we are currently observing
    user_1h_window = (Window.partitionBy("user_id").orderBy("ts_unix").rangeBetween(-3600, 0))

    # Count all user transactions last hour
    df = df.withColumn(
        "user_transaction_count_1h",
        F.count("*").over(user_1h_window)
    )

    # Count declined transactions in last hours
    df = df.withColumn(
        "user_decline_count_1h",
        F.sum(F.when(F.col("transaction_status") == "Declined", 1).otherwise(0)).over(user_1h_window)
    )

    # Decline rate = declined / total. A high rate would point to card probing
    df = df.withColumn(
        "user_decline_rate_1h",
        F.col("user_decline_count_1h") / F.col("user_transaction_count_1h")
    )

    # Count unique payment methods used in the window. High number also likely for probing
    df = df.withColumn(
        "user_unique_payment_methods_1h",
        F.approx_count_distinct("payment_id").over(user_1h_window)
    )

    ######################################### USER 5 min WINDOW ################################################
    user_5min_window = (Window.partitionBy("user_id").orderBy("ts_unix").rangeBetween(-300, 0))

    # 5 min window for botting, we could probably reduce this window length to 2-3 mins
    df = df.withColumn(
        "user_transaction_count_5min",
        F.count("*").over(user_5min_window)
    )

    ######################################### Device 5 min WINDOW ################################################
    device_5min_window = (Window.partitionBy("device_id").orderBy("ts_unix").rangeBetween(-300, 0))

    df = df.withColumn(
        "device_transaction_count_5min",
        F.count("*").over(device_5min_window)
    )

    df = df.drop("ts_unix") # Drop helper col

    return df