from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def compute_amount_features(df: DataFrame) -> DataFrame:
    """
    Computes amount-based features for each transaction. Amount features capture
    whether a transaction amount is unusual relative to a user's historical spending behavior.

    Features computed:
        - user_avg_amount_24h: Average transaction amount for the user in the last 24 hours
        - user_stddev_amount_24h: Standard deviation of transaction amounts for the user in the last 24 hours
        - user_amount_ratio_24h: Ratio of current transaction amount to user's 24h average
        - user_avg_amount_7d: Average transaction amount for the user in the last 7 days
        - user_amount_ratio_7d: Ratio of current transaction amount to user's 7 day average

    Args:
        df (DataFrame): Raw transactions DataFrame. Expected columns: user_id, transaction_amount_usd, transaction_timestamp
    Returns:
        DataFrame: Original DataFrame with amount feature columns appended.
    """
    # We convert the timestamp to unix seconds so we can use rangeBetween (uses numeric offsets)
    df = df.withColumn("ts_unix", F.unix_timestamp("transaction_timestamp"))

    ######################################### USER 24h WINDOW ################################################
    user_24h_window = (Window.partitionBy("user_id").orderBy("ts_unix").rangeBetween(-86400, 0)) # 24 * 60 * 60 = 86400

    # Average transaction amount in last 24h
    df = df.withColumn(
        "user_avg_amount_24h",
        F.avg("transaction_amount_usd").over(user_24h_window)
    )

    # Standard deviation of transaction amounts in last 24h
    df = df.withColumn(
        "user_stddev_amount_24h",
        F.stddev("transaction_amount_usd").over(user_24h_window)
    )

    # Ratio of current amount to 24h average | A ratio >> 1 means transaction is much larger than usual
    df = df.withColumn(
        "user_amount_ratio_24h",
        F.col("transaction_amount_usd") / F.when(
            F.col("user_avg_amount_24h") == 0, None).otherwise(F.col("user_avg_amount_24h"))
    )

    ######################################### USER 7 DAY WINDOW ################################################
    user_7d_window = (Window.partitionBy("user_id").orderBy("ts_unix").rangeBetween(-604800, 0))

    # Average transaction amount in last 7 days
    df = df.withColumn(
        "user_avg_amount_7d",
        F.avg("transaction_amount_usd").over(user_7d_window)
    )

    # Ratio of current amount to 7 day average
    df = df.withColumn(
        "user_amount_ratio_7d",
        F.col("transaction_amount_usd") / F.when(
            F.col("user_avg_amount_7d") == 0, None).otherwise(F.col("user_avg_amount_7d"))
    )

    df = df.drop("ts_unix") # Drop helper col

    return df