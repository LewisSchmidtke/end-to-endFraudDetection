import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import src.CurrencyConvertor as CC
import src.TransactionGenerator as TG
import src.DatabaseManager as DBM


def serialize(data: dict) -> bytes:
    """
    Serializes a transaction dictionary to JSON bytes, handling datetime objects via default=str.

    Args:
        data (dict): Transaction dictionary to serialize
    Returns:
        bytes: JSON encoded bytes
    """
    return json.dumps(data, default=str).encode("utf-8")


def run_producer(transactions_per_second: float = 1.0) -> None:
    """
    Continuously generates transaction patterns and publishes them to the Kafka 'transactions' topic.
    Fetches merchant IDs from Postgres on startup, then generates patterns for randomly selected users and devices at
    the specified rate.

    Args:
        transactions_per_second (float): Target publish rate. Defaults to 1.0.
    Returns:
        None
    """
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=serialize,
    )

    CurrencyConvertor = CC.CurrencyConvertor()
    conversion_rates = CurrencyConvertor.fetch_conversion_rates()
    TransactionGen = TG.TransactionGenerator(conversion_rates=conversion_rates)
    DBManager = DBM.DatabaseManager()

    merchant_ids = DBManager.fetch_all_merchant_ids()
    sleep_time = 1.0 / transactions_per_second

    print(f"Producer started publishing {transactions_per_second} tx/s")

    while True:
        user_id = DBManager.fetch_random_user_id()
        device_id = DBManager.fetch_random_device_id(user_id)
        merchant_id = random.choice(merchant_ids)
        pattern_start_time = datetime.now()

        transactions = TransactionGen.generate_transaction_pattern(
            user_id=user_id,
            device_id=device_id,
            merchant_id=merchant_id,
            pattern_start_time=pattern_start_time,
        )

        for transaction in transactions:
            # We have to extract the corresponding data for the payment method because we need
            # the creation time for feature compute
            payment_info = DBManager.fetch_payment_info(transaction["payment_id"])
            transaction["payment_created_at"] = payment_info["created_at"]
            producer.send("transactions", value=transaction)

        producer.flush()
        time.sleep(sleep_time)


if __name__ == "__main__":
    run_producer(transactions_per_second=1.0)