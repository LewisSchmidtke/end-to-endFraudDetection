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
│   └── init.sql                        # PostgreSQL schema (users, devices, payments, merchants, transactions)
├── ml/
│   └── models/
│       ├── xgb.py                      # XGBoost classifier builder
│       └── random_forest.py            # Random Forest classifier builder
├── spark/
│   ├── features/
│   │   ├── velocity_features.py        # Transaction velocity (1h, 5min windows)
│   │   ├── amount_features.py          # Spending amount features (24h, 7d windows)
│   │   ├── behavioral_features.py      # Behavioral anomaly features (24h, 30d windows)
│   │   └── device_features.py          # Device and payment method features
│   ├── jobs/
│   │   └── feature_engineering_job.py  # Batch feature engineering entry point
│   └── utils/
│       ├── spark_session.py            # SparkSession factory
│       └── db_utils.py                 # JDBC read/write helpers
├── src/
│   ├── constants.py                    # All configuration constants and model params
│   ├── CurrencyConvertor.py            # Live exchange rate fetching (ExchangeRate-API)
│   ├── DatabaseManager.py              # PostgreSQL CRUD operations via psycopg2
│   ├── DataGenerator.py                # Synthetic user, device, merchant, payment generators
│   ├── TransactionGenerator.py         # Transaction and fraud pattern generation
│   └── utility.py                      # Shared utility functions
├── generate_data.py                    # Main data generation script
└── docker-compose.yml                  # PostgreSQL + Spark cluster setup
```

---

## Pipeline Components

### 1. Synthetic Data Generation (`src/DataGenerator.py`)

Four generator classes create synthetic but realistic entities using configurable weighted distributions defined in `src/constants.py`:

- **`UserGenerator`** — Creates users with weighted country distributions, fake names, emails, and geolocations via Faker.
- **`DeviceGenerator`** — Generates devices (mobile, desktop, tablet) to users.
- **`PaymentMethodGenerator`** — Generates payment methods (credit card, debit card, bank transfer, BNPL, crypto) with weighted provider tiers (Renowned, Mid, Unknown).
- **`MerchantGenerator`** — Creates merchants with categories (Groceries, Electronics, Restaurants, Travel, Clothing, Gift Cards, Healthcare, Other) and quality ratings.

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
- **Card Cracking** — Systematic testing of card credentials
- **Account Takeover** — Sudden activity spike after long inactivity, potentially from a new location with a new payment method
- **Merchant Switching** — Payment method suddenly used across unusual merchant categories (e.g. gift cards)

### 3. Database (`db/init.sql`, `src/DatabaseManager.py`)

The `DatabaseManager` class handles all PostgreSQL interactions via `psycopg2`, including inserting all entity types, 
fetching and deactivating payment methods and random ID selection for transaction generation.

The database schema can be seen here:

![Database Schema](data/images/db_schema.png?raw=true)

### 4. Infrastructure (`docker-compose.yml`)

Three services are defined on a shared `fraud_network` bridge network, allowing containers to communicate with each other 
by service name rather than hardcoded IPs.

| Service      | Image              | Ports                         |
|--------------|--------------------|-------------------------------|
| PostgreSQL   | postgres:16        | 5432                          |
| Spark Master | apache/spark:3.5.3 | 8080 (Web UI), 7077 (cluster) |
| Spark Worker | apache/spark:3.5.3 | —                             |

**PostgreSQL** is the central data store for all generated entities and transactions. Port 5432 is exposed to the host 
so local Python scripts (e.g. `generate_data.py`) can connect directly. Inside the network, Spark jobs reach it via 
the `db` hostname, which is why `db_utils.py` supports a separate `POSTGRES_HOST_DOCKER` environment variable.

**Spark Master** acts as the cluster coordinator. It exposes port 7077 for worker and job connections, and port 8080 
for the Spark web UI where running jobs and worker status can be monitored.

**Spark Worker** is the compute node that executes jobs assigned by the master. It connects to the master on startup 
via `spark://spark-master:7077` and is configured with 2 CPU cores and 2GB of memory. Additional workers can be added 
to the compose file to scale out compute.

### 5. Feature Engineering (`spark/features/`, `spark/jobs/`)

All features are computed using PySpark window functions and written to Parquet via `spark/jobs/feature_engineering_job.py`. 
Features are designed to catch the specific patterns introduced during data generation.

| Feature Group      | Key Features                                                                                                                                          |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Velocity**       | `user_transaction_count_1h`, `user_decline_rate_1h`, `user_unique_payment_methods_1h`, `user_transaction_count_5min`, `device_transaction_count_5min` |
| **Amount**         | `user_avg_amount_24h`, `user_stddev_amount_24h`, `user_amount_ratio_24h`, `user_avg_amount_7d`, `user_amount_ratio_7d`                                |
| **Behavioral**     | `seconds_since_last_transaction`, `user_unique_merchants_24h`, `user_unique_countries_24h`, `is_new_merchant_category`                                |
| **Device/Payment** | `device_unique_users_24h`, `device_unique_payment_methods_24h`, `payment_method_age_days`, `is_new_payment_method`                                    |

### 6. Machine Learning (`ml/models/`)

Two classifiers are available, both configurable via `src/constants.py`. XGBoost uses `aucpr` (area under the precision-recall curve) as its eval metric, suited for the imbalanced fraud dataset. Both models support either direct class weighting or SMOTE-based balancing.

| Model         | File                         | Imbalance Handling                 |
|---------------|------------------------------|------------------------------------|
| XGBoost       | `ml/models/xgb.py`           | `scale_pos_weight` or SMOTE        |
| Random Forest | `ml/models/random_forest.py` | `class_weight="balanced"` or SMOTE |

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
python generate_data.py
```

Default configuration generates 50 merchants and 250 users, each with 3–12 transaction patterns.

### Run Feature Engineering

```bash
python -m spark.jobs.feature_engineering_job --mode batch
```

Output is written to `data/features/transactions_features.parquet`.

---

## Status

| Component                        | Status         |
|----------------------------------|----------------|
| Synthetic data generation        | ✅ Complete     |
| PostgreSQL schema & integration  | ✅ Complete     |
| Normal transaction patterns      | ✅ Complete     |
| Card Probing fraud pattern       | ✅ Complete     |
| Botting fraud pattern            | ✅ Complete     |
| Spark feature engineering        | ✅ Complete     |
| Model training (XGBoost / RF)    | 🚧 In progress |
| PyTorch custom model             | 🚧 In progress |
| Kafka data streaming             | 🔜 Planned     |
| ONNX export & Triton inference   | 🔜 Planned     |
| Card Cracking fraud pattern      | 🔜 Planned     |
| Account Takeover fraud pattern   | 🔜 Planned     |
| Merchant Switching fraud pattern | 🔜 Planned     |