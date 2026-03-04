# E2E Fraud Detection

## Overview
This project implements an end-to-end fraud detection pipeline, spanning from synthetic transaction pattern generation, 
over data storage and feature engineering, to model training and inference. The core development was done in 
<span style="color:red">Python</span>. <span style="color:red">SQL</span> is run through <span style="color:red">Docker</span> 
for data storage and database operations. <span style="color:red">Apache Kafka</span> and <span style="color:red">Apache Spark</span> 
are used for data ingestion and feature engineering. The machine learning aspect is handled through <span style="color:red">PyTorch</span> 
and <span style="color:red">Scikit-Learn</span> for a mix between custom and out-of-the-box models. Finally, 
<span style="color:red">NVIDIA Triton</span> and <span style="color:red">ONNX</span> are used to deploy the model for inference.

The system design can be seen here:

![System Design](data/images/system_architecture.png?raw=true)

---

## Project Structure

```
├── db/
│   └── init.sql                        # PostgreSQL schema (users, devices, payments, merchants, transactions, fraud_alerts)
├── ml/
│   ├── models/
│   │   ├── xgb.py                      # XGBoost classifier builder
│   │   ├── random_forest.py            # Random Forest classifier builder
│   │   ├── pytorch_model.py            # FraudNet built with PyTorch
│   │   ├── pytorch_wrapper.py          # Sklearn-compatible wrapper for FraudNet
│   │   └── model_lib.py                # Model registry for train/evaluate scripts
│   ├── datasets.py                     # FraudDataset and TorchFraudDataset
│   ├── train.py                        # Model training entry point (CLI)
│   └── evaluate.py                     # Model evaluation entry point (CLI)
├── spark/
│   ├── features/
│   │   ├── velocity_features.py        # Transaction velocity (1h, 5min windows)
│   │   ├── amount_features.py          # Spending amount features (24h, 7d windows)
│   │   ├── behavioral_features.py      # Behavioral anomaly features (24h, 30d windows)
│   │   └── device_features.py          # Device and payment method features
│   ├── jobs/
│   │   ├── batch_job.py                # Batch feature engineering entry point
│   │   └── streaming_job.py            # Kafka streaming inference pipeline
│   └── utils/
│       ├── spark_utils.py              # SparkSession factory and Row conversion helpers
│       ├── db_utils.py                 # JDBC read/write helpers
│       └── message_utils.py            # Kafka transaction message schema
├── src/
│   ├── constants.py                    # All configuration constants and model params
│   ├── CurrencyConvertor.py            # Live exchange rate fetching (ExchangeRate-API)
│   ├── DatabaseManager.py              # PostgreSQL CRUD operations via psycopg2
│   ├── DataGenerator.py                # Synthetic user, device, merchant, payment generators
│   ├── TransactionGenerator.py         # Transaction and fraud pattern generation
│   └── utility.py                      # Shared utility functions
├── scripts/
│   ├── init_data.py                    # Initial batch data generation script
│   └── kafka_producer.py               # Kafka transaction producer for streaming
└── docker-compose.yml                  # PostgreSQL + Spark + Kafka cluster setup
```

---

## Pipeline Components

### 1. Synthetic Data Generation (`src/DataGenerator.py`)

Four generator classes create synthetic but realistic entities using configurable weighted distributions defined in `src/constants.py`:

- **`UserGenerator`**: Creates users with weighted country distributions, fake names, emails and geolocations via Faker.
- **`DeviceGenerator`**: Generates devices (mobile, desktop, tablet) linked to users.
- **`PaymentMethodGenerator`**: Generates payment methods (credit card, debit card, bank transfer, BNPL, crypto) with weighted provider tiers (Renowned, Mid, Unknown).
- **`MerchantGenerator`**: Creates merchants with categories (Groceries, Electronics, Restaurants, Travel, Clothing, Gift Cards, Healthcare, Other) and quality ratings.

### 2. Transaction Generation (`src/TransactionGenerator.py`)

The `TransactionGenerator` class produces chronological transaction patterns per user, both normal and fraudulent. 
Each pattern is anchored to a `TransactionContext` dataclass that carries all static fields, with dynamic fields 
(timestamp, payment ID, status) filled in during pattern generation.

**Transaction amount clusters:**

| Cluster             | Range                 | Distribution |
|---------------------|-----------------------|--------------|
| Mini Level Spending | $0.50 – $10           | Uniform      |
| Low Level Spending  | $11 – $100            | Uniform      |
| Mid Level Spending  | $101 – $10,000        | Trapezoidal  |
| High Level Spending | $10,001 – $10,000,000 | Exponential  |

**Implemented fraud patterns:**

| Fraud Type   | Description                                               | Transactions | Interval | Approval Rate |
|--------------|-----------------------------------------------------------|--------------|----------|---------------|
| Card Probing | Many new cards tested with small amounts, mostly declined | 3–15         | 30–90s   | 10%           |
| Botting      | Rapid repeat purchases with the same payment method       | 1–10         | 0.5–1s   | 90%           |

**Planned fraud patterns:**
- **Card Cracking**: Systematic testing of card credentials
- **Account Takeover**: Sudden activity spike after long inactivity, potentially from a new location with a new payment method
- **Merchant Switching**: Payment method suddenly used across unusual merchant categories (e.g. gift cards)

### 3. Database (`db/init.sql`, `src/DatabaseManager.py`)

The `DatabaseManager` class handles all PostgreSQL interactions via `psycopg2`, including inserting all entity types, 
fetching and deactivating payment methods, random ID selection for transaction generation, fetching user and device 
transaction history for streaming feature computation and inserting fraud alerts.

The database schema includes a `fraud_alerts` table (in addition to users, user_devices, payment_methods, merchants 
and transactions) 
to track model-generated alerts from the streaming pipeline.

The database schema can be seen here:

![Database Schema](data/images/db_schema.png?raw=true)

### 4. Infrastructure (`docker-compose.yml`)

Six services are defined on a shared `fraud_network` bridge network, allowing containers to communicate with each other 
by service name rather than hardcoded IPs.

| Service      | Image                           | Ports                         |
|--------------|---------------------------------|-------------------------------|
| PostgreSQL   | postgres:16                     | 5432                          |
| Spark Master | apache/spark:3.5.3              | 8080 (Web UI), 7077 (cluster) |
| Spark Worker | apache/spark:3.5.3              | —                             |
| Zookeeper    | confluentinc/cp-zookeeper:7.6.0 | 2181                          |
| Kafka        | confluentinc/cp-kafka:7.6.0     | 9092                          |
| Kafka Setup  | confluentinc/cp-kafka:7.6.0     | —                             |

**PostgreSQL** is the central data store for all generated entities and transactions. Port 5432 is exposed to the host 
so local Python scripts can connect directly. Inside the network, Spark jobs reach it via the `db` hostname.

**Spark Master** acts as the cluster coordinator. It exposes port 7077 for worker and job connections and port 8080 
for the Spark web UI where running jobs and worker status can be monitored.

**Spark Worker** is the compute node that executes jobs assigned by the master. It connects to the master on startup 
via `spark://spark-master:7077` and is configured with 2 CPU cores and 2GB of memory.

**Kafka** (with Zookeeper) handles real-time transaction ingestion. Two topics are created automatically by the 
`kafka-setup` service on startup: `transactions` (3 partitions) for incoming transaction events and `fraud_alerts` 
(3 partitions) for publishing model-scored alerts.

### 5. Feature Engineering (`spark/features/`, `spark/jobs/`)

All features are computed using PySpark window functions. The batch job reads from PostgreSQL and writes to Parquet via 
`spark/jobs/batch_job.py`. The streaming job in `spark/jobs/streaming_job.py` reuses the same feature functions against 
a rolling window of user/device history fetched from Postgres, guaranteeing identical features between batch and 
streaming pipelines.

| Feature Group      | Key Features                                                                                                                                                  |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Velocity**       | `user_transaction_count_1h`, `user_decline_rate_1h`, `user_unique_payment_methods_1h`, `user_transaction_count_5min`, `device_transaction_count_5min`         |
| **Amount**         | `user_avg_amount_24h`, `user_stddev_amount_24h`, `user_amount_ratio_24h`, `user_avg_amount_7d`, `user_amount_ratio_7d`                                        |
| **Behavioral**     | `seconds_since_last_transaction`, `user_unique_merchants_24h`, `user_unique_countries_24h`, `user_unique_merchant_categories_24h`, `is_new_merchant_category` |
| **Device/Payment** | `device_unique_users_24h`, `device_unique_payment_methods_24h`, `payment_method_age_days`, `is_new_payment_method`                                            |

### 6. Machine Learning (`ml/`)

Three models are available, all configurable via `src/constants.py` and registered in `ml/models/model_lib.py`. XGBoost 
uses `aucpr` (area under the precision-recall curve) as its eval metric, suited for the imbalanced fraud dataset. 
All models support either direct class weighting or SMOTE-based balancing.

| Model              | File                           | Imbalance Handling                         |
|--------------------|--------------------------------|--------------------------------------------|
| XGBoost            | `ml/models/xgb.py`             | `scale_pos_weight` or SMOTE                |
| Random Forest      | `ml/models/random_forest.py`   | `class_weight="balanced"` or SMOTE         |
| PyTorch (FraudNet) | `ml/models/pytorch_wrapper.py` | `pos_weight` in BCEWithLogitsLoss or SMOTE |

**FraudNet** (`ml/models/pytorch_model.py`) is a feed-forward neural network with three blocks of 
`Linear → BatchNorm1d → ReLU → Dropout(0.2)`, reducing from the input size down to 64 → 32 → 16 → 1 output neuron.

**Training** (`ml/train.py`) loads the parquet feature file, handles class imbalance, fits the chosen model and saves 
both the model and its `StandardScaler` to `data/models/`. A `feature_columns.joblib` file is also saved on first run 
to guarantee consistent feature ordering at inference time.

**Evaluation** (`ml/evaluate.py`) loads a saved model, runs predictions on the held-out test set, finds the optimal F1 
threshold on the precision-recall curve and saves per-model plots (PR curve, confusion matrix, feature importances) 
and a `metrics_summary.csv` to `data/evaluation/`.

### 7. Kafka Streaming (`scripts/kafka_producer.py`, `spark/jobs/streaming_job.py`)

The `kafka_producer.py` script continuously generates transaction patterns for randomly selected users and publishes 
them to the `transactions` Kafka topic at a configurable rate (default 1 tx/s).

The `streaming_job.py` Spark Structured Streaming job reads from the `transactions` topic, enriches each micro-batch with 
historical context from Postgres, computes the full feature set using the same Spark feature functions as the batch 
pipeline, scores each transaction with the loaded model and writes fraud alerts to both the `fraud_alerts` Kafka topic 
and the `fraud_alerts` Postgres table.

---

## Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- A valid [ExchangeRate API key](https://www.exchangerate-api.com/)

### Configuration

Create a `credentials.env` file in the project root (git-ignored):

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=<your_db_name>
POSTGRES_USER=<your_user>
POSTGRES_PASSWORD=<your_password>
ExchangeRateApiKey=<your_api_key>
```

### Start Infrastructure

```bash
docker-compose up -d
```

### Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Generate Synthetic Data

```bash
python scripts/init_data.py
```

Default configuration generates 50 merchants and 250 users, each with 3–12 transaction patterns.

### Run Batch Feature Engineering

```bash
python -m spark.jobs.batch_job
```

Output is written to `data/features/transactions_features.parquet`.

### Train Models

```bash
# Train all models
python -m ml.train --model all

# Train a specific model with SMOTE
python -m ml.train --model xgb --smote

# Train PyTorch model with custom hyperparameters
python -m ml.train --model pytorch --epochs 100 --batch-size 512 --lr 5e-4
```

### Evaluate Models

```bash
# Evaluate all saved models
python -m ml.evaluate --model all

# Evaluate a specific model
python -m ml.evaluate --model xgb
```

Reports are saved to `data/evaluation/`.

### Run Kafka Streaming Pipeline

```bash
# Start the transaction producer
python scripts/kafka_producer.py

# Start the streaming inference job (in a separate terminal)
python -m spark.jobs.streaming_job
```

---

## Status

| Component                       | Status         |
|---------------------------------|----------------|
| Synthetic data generation       | ✅ Complete     |
| PostgreSQL schema & integration | ✅ Complete     |
| Normal transaction patterns     | ✅ Complete     |
| Card Probing fraud pattern      | ✅ Complete     |
| Botting fraud pattern           | ✅ Complete     |
| Spark feature engineering       | ✅ Complete     |
| Model training (XGBoost / RF)   | ✅ Complete     |
| PyTorch FraudNet model          | ✅ Complete     |
| Model evaluation & reporting    | ✅ Complete     |
| Kafka data streaming            | ✅ Complete     |
| Streaming inference pipeline    | ✅ Complete     |
| ONNX export & Triton inference  | 🔜 Planned     |
| More fraud patterns             | 🔜 Planned     |