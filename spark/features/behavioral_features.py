from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def compute_behavioral_features(df: DataFrame, merchants_df: DataFrame) -> DataFrame:
    """
    Computes behavioral features for each transaction. Behavioral features capture
    whether a user is acting differently from their established patterns.

    Features computed:
        - seconds_since_last_transaction: Time in seconds since the user's previous transaction.
          A very short gap suggests botting, a very long gap followed by unusual activity suggests account takeover.
        - user_unique_merchants_24h: Number of distinct merchants the user transacted with in the last 24 hours
        - user_unique_countries_24h: Number of distinct countries the user transacted in the last 24 hours
        - user_unique_merchant_categories_24h: Number of distinct merchant categories in the last 24 hours
        - is_new_merchant_category: 1 if the current transaction's merchant category has not been seen
          in the user's last 30 days of transactions, 0 otherwise. Key signal for merchant switching fraud.

    Args:
        df (DataFrame): Raw transactions DataFrame. Expected columns: user_id, transaction_timestamp, transaction_country, merchant_id
        merchants_df (DataFrame): Merchants reference DataFrame. Expected columns: merchant_id, merchant_category
    Returns:
        DataFrame: Original DataFrame with behavioral feature columns appended.
    """
    # We convert the timestamp to unix seconds so we can use rangeBetween (uses numeric offsets)
    df = df.withColumn("ts_unix", F.unix_timestamp("transaction_timestamp"))

    if "merchant_category" not in df.columns:
        df = df.join(merchants_df.select("merchant_id", "merchant_category"), on="merchant_id", how="left")

    ######################################### TIME SINCE LAST TRANSACTION ################################################
    # We get all user transactions and then look at the last transaction time
    user_time_window = Window.partitionBy("user_id").orderBy("ts_unix")

    df = df.withColumn(
        "seconds_since_last_transaction",
        F.col("ts_unix") - F.lag("ts_unix", 1).over(user_time_window)
    )
    # First transaction for a user will have null here since there's no previous row

    ######################################### USER 24 hour BEHAVIORAL WINDOW ################################################
    user_24h_window = (Window.partitionBy("user_id").orderBy("ts_unix").rangeBetween(-86400, 0))

    # Count unique merchants
    df = df.withColumn(
        "user_unique_merchants_24h",
        F.approx_count_distinct("merchant_id").over(user_24h_window)
    )

    # Count transaction countries
    df = df.withColumn(
        "user_unique_countries_24h",
        F.approx_count_distinct("transaction_country").over(user_24h_window)
    )

    # Count unique merchant categories
    df = df.withColumn(
        "user_unique_merchant_categories_24h",
        F.approx_count_distinct("merchant_category").over(user_24h_window)
    )

    ######################################### NEW MERCHANT CATEGORY (30 day) ##########################################
    # For each transaction we want to know: has this user visited this merchant category in the last 30 days
    # We use -1 as the upper bound to exclude the current row | 30 days = 30 * 24 * 60 * 60 = 2592000 seconds
    user_30d_window_excl = (Window.partitionBy("user_id").orderBy("ts_unix").rangeBetween(-2592000, -1))

    # Collect all merchant categories seen in the last 30 days into an array
    df = df.withColumn(
        "_seen_categories_30d",
        F.collect_set("merchant_category").over(user_30d_window_excl)
    )

    # Check if current merchant category is NOT in the seen categories array -> sets 1 if not in seen merchants
    df = df.withColumn(
        "is_new_merchant_category",
        F.when(F.array_contains(F.col("_seen_categories_30d"), F.col("merchant_category")),0).otherwise(1).cast("integer")
    )

    df = df.drop("ts_unix", "_seen_categories_30d") # Drop helper col

    return df