from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


TRANSACTION_MESSAGE_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("device_id", IntegerType()),
    StructField("merchant_id", IntegerType()),
    StructField("payment_id", IntegerType()),
    StructField("transaction_amount_usd", FloatType()),
    StructField("transaction_amount_local", FloatType()),
    StructField("transaction_currency", StringType()),
    StructField("transaction_country", StringType()),
    StructField("transaction_channel", StringType()),
    StructField("transaction_status", StringType()),
    StructField("transaction_timestamp", StringType()),
    StructField("is_fraudulent", IntegerType()),
    StructField("fraud_type", StringType()),
    StructField("payment_created_at", StringType()),  # Needed for payment_method_age_days
])