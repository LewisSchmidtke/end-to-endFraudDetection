import json
import joblib
import numpy as np
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
import pyspark.sql.functions as F

from spark.utils.spark_utils import create_spark_session
from spark.utils.spark_utils import convert_dicts_to_spark_rows
from spark.utils.spark_utils import filter_single_transaction
from spark.utils.message_utils import TRANSACTION_MESSAGE_SCHEMA
from spark.features.velocity_features import compute_velocity_features
from spark.features.amount_features import compute_amount_features
from spark.features.behavioral_features import compute_behavioral_features
from spark.features.device_features import compute_device_features

from src.DatabaseManager import DatabaseManager
from src.constants import MODEL_OUTPUT_DIR, MERCHANT_CATEGORY_DATA, ONLINE_TX_CHANNEL


ROOT = Path(__file__).resolve().parent.parent.parent
MODEL_DIR = ROOT / MODEL_OUTPUT_DIR


def _compute_streaming_features(
        transaction: dict,
        dbm: DatabaseManager,
        spark: SparkSession,
        feature_column_list: list[str]
) -> np.ndarray | None:
    """
    Computes features for a single incoming transaction by combining it with historical records from Postgres and
    running the existing batch feature functions. Guarantees identical features between batch and streaming pipelines.

    Args:
        transaction (dict): Incoming transaction dictionary from Kafka
        dbm (DatabaseManager): DatabaseManager instance
        spark (SparkSession): Active SparkSession used to create the historical DataFrame
        feature_column_list (list[str]): List of feature_names that will be used to construct the feature vector
    Returns:
        np.ndarray | None: Computed feature vector or None if computation fails.
    """
    try:
        # We extract user history from past 720 hours (30 days) as this is the max window length we check with spark
        user_history = dbm.fetch_user_transaction_history(transaction["user_id"], hours=720)
        # We need the merchant category for latest transaction
        merchant_info = dbm.fetch_merchant_info(transaction["merchant_id"])
        transaction["merchant_category"] = merchant_info["merchant_category"]

        transaction_filtered = filter_single_transaction(transaction)
        user_history.append(transaction_filtered)

        # We extract the merchant historical data, because a separate df is needed for 'compute_behavioral_features'
        merchant_history = [{"merchant_id": r["merchant_id"], "merchant_category": r["merchant_category"]} for r in user_history]
        # We extract the payment info from the current transaction, because a separate df is needed for device features
        payment_method_history = [
            {"payment_method_id": transaction["payment_id"],
             "created_at": datetime.fromisoformat(str(transaction["payment_created_at"]))}
        ]
        # We first have to convert the dict into spark native Row types
        user_history = convert_dicts_to_spark_rows(user_history)

        # Distinct call not on payment because its only 1 row | distinct is equivalent to pandas drop_duplicates()
        transactions_df = spark.createDataFrame(user_history).distinct()
        merchants_df = spark.createDataFrame(convert_dicts_to_spark_rows(merchant_history)).distinct()
        payment_methods_df = spark.createDataFrame(convert_dicts_to_spark_rows(payment_method_history))

        df = compute_velocity_features(transactions_df)
        df = compute_amount_features(df)
        df = compute_behavioral_features(df, merchants_df)
        df = compute_device_features(df, payment_methods_df)

        # We need one hot encoding and binary encoding
        for cat in MERCHANT_CATEGORY_DATA:
            df = df.withColumn(
                f"merchant_category_{cat}",
                F.when(F.col("merchant_category") == cat, 1).otherwise(0).cast("integer")
            )
        df = df.drop("merchant_category")

        df = df.withColumn(
            "transaction_channel",
            F.when(F.col("transaction_channel") == ONLINE_TX_CHANNEL, 1).otherwise(0).cast("integer")
        )

        # Extract the last row which corresponds to the incoming transaction and build the feature vector from previously
        # saved feature_columns file.
        last_row = df.orderBy("transaction_timestamp").tail(1)[0]
        feature_vector = np.array([last_row[f_column] for f_column in feature_column_list],dtype=np.float32)

        print(f"Feature computation completed for user {transaction.get('user_id')}")
        return feature_vector

    except Exception as e:
        print(f"Feature computation failed for user {transaction.get('user_id')}: {e}")
        return None


def _process_batch(
        batch_df: DataFrame,
        batch_id: int,
        spark: SparkSession,
        model,
        model_name: str,
        scaler,
        feature_column_list: list[str],
        alert_producer: KafkaProducer,
) -> None:
    """
    Processes a micro-batch of transactions from Kafka. Computes features, scores each transaction and prints fraud
    alerts. Called by foreachBatch on each micro-batch.

    Args:
        batch_df (DataFrame): Micro-batch DataFrame from Kafka.
        batch_id (int): Micro-batch ID assigned by Spark.
        spark (SparkSession): Active SparkSession
        model: Trained fraud detection model with predict_proba() method
        model_name (str): Name of the model currently running inference
        scaler: Fitted StandardScaler matching the training pipeline
        feature_column_list (list[str]): List of feature_names that will be used to construct the feature vector
        alert_producer (KafkaProducer): Kafka producer used to publish fraud alerts to the fraud_alerts topic
    Returns:
        None
    """
    if batch_df.isEmpty():
        return

    dbm = DatabaseManager()
    transactions = [row.asDict() for row in batch_df.collect()]

    for transaction in transactions:
        dbm.insert_transaction(transaction)
        features = _compute_streaming_features(transaction, dbm, spark, feature_column_list)
        if features is None:
            continue

        scaled = scaler.transform(features.reshape(1, -1))
        fraud_prob = float(model.predict_proba(scaled)[0][1])
        is_fraud = int(fraud_prob >= 0.5)
        print(fraud_prob, is_fraud)
        if is_fraud:
            alert = {
                "transaction_id": transaction.get("transaction_id"),
                "user_id": transaction.get("user_id"),
                "fraud_probability": fraud_prob,
                "model_name": model_name,
                "alerted_at": datetime.now().isoformat(),
            }

            alert_producer.send("fraud_alerts", value=alert) # Write to Kafka fraud_alerts topic

            dbm.insert_fraud_alert(alert)

            print(f"FRAUD ALERT: {alert}")

    alert_producer.flush()  # Flush once after all transactions in batch are processed


def run_streaming(model_name: str = "xgb") -> None:
    """
    Loads a trained model, scaler and feature column list, reads transactions from the Kafka transactions topic,
    computes features and scores each transaction, writing fraud alerts to the fraud_alerts topic and Postgres.

    Args:
        model_name (str): Model to use for scoring. Must be a key in MODEL_LIB. Defaults to 'xgb'
    Returns:
        None
    """
    spark = create_spark_session(
        app_name="FraudDetection_Streaming",
        master="local[*]"
    )

    # Load model, scaler and feature column list
    model = joblib.load(MODEL_DIR / f"{model_name}.joblib")
    scaler = joblib.load(MODEL_DIR / f"{model_name}_scaler.joblib")
    feature_column_list = joblib.load(MODEL_DIR / "feature_columns.joblib")

    # Read from kafka and parse json
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .load()
    )
    parsed_stream = (
        raw_stream
        .select(from_json(col("value").cast("string"), TRANSACTION_MESSAGE_SCHEMA).alias("data"))
        .select("data.*")
    )

    alert_producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8")
    )

    # We wrap _process_batch in lambda because it doesnt match the function signature of foreachBatch. With lambda,
    # we have access to the previously calculated variables in the scope and can therefore call _process_batch inside
    # foreachBatch
    query = (
        parsed_stream.writeStream
        .foreachBatch(lambda df, batch_id:
                              _process_batch(df, batch_id, spark, model, model_name, scaler, feature_column_list, alert_producer))
        .option("checkpointLocation", "/tmp/fraud_checkpoint")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_streaming(model_name="xgb")