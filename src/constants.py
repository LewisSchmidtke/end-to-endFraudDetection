# Country data for User, Merchant and Transaction generation
COUNTRY_DATA = {
    "US" : {"currency" : "USD", "weight" : 0.5},
    "CN" : {"currency" : "CNY", "weight" : 0.15},
    "IN" : {"currency" : "INR", "weight" : 0.05},
    "CA" : {"currency" : "CAD", "weight" : 0.025},
    "JP" : {"currency" : "JPY", "weight" : 0.025},
    "DE" : {"currency" : "EUR", "weight" : 0.075},
    "GB" : {"currency" : "GBP", "weight" : 0.1},
    "FR" : {"currency" : "EUR", "weight" : 0.075}
}

# Email data for User generation
EMAIL_DATA = {
    "free" : {"provider" : "@free.com", "weight" : 0.7},
    "premium" : {"provider" : "@premium.com", "weight" : 0.1},
    "business" : {"provider" : "@business.com", "weight" : 0.2},
}

# Payment data for payment method generation
PAYMENT_METHOD_DATA = {
    "bank_transfer" : {"weight": 0.23},
    "credit_card" : {"weight": 0.30},
    "debit_card" : {"weight" : 0.36},
    "BNPL" : {"weight" : 0.1},
    "crypto_currency" : {"weight" : 0.01},
}

# Payment data for payment method generation
PAYMENT_PROVIDER_DATA = {
    "Renowned" : {"weight": 0.75},
    "Mid" : {"weight": 0.24},
    "Unknown" : {"weight": 0.01},
}

# Merchant data for merchant generation
MERCHANT_DATA = {
    "Renowned": {"weight": 0.7},
    "Mid": {"weight": 0.25},
    "Unknown": {"weight": 0.05},
}

# Transaction amount cluster data for transaction generation
TRANSACTION_CLUSTER_DATA = {
    "Mini Level Spending" : {"min" : 0.50, "max" : 10, "weight" : 0.35, "distribution_function" : "random"},
    "Low Level Spending" : {"min" : 11, "max" : 100, "weight" : 0.5, "distribution_function" : "random"},
    "Mid Level Spending" : {"min" : 101, "max" : 10000, "weight" : 0.125, "distribution_function" : "trapezoidal"},
    "High Level Spending" : {"min" : 10001, "max" : 10000000, "weight" : 0.025, "distribution_function" : "exp"}
}

# Fraud type distribution for fraudulent transactions
# Min/Max transactions are the minimum and maximum number of transaction that will be generated in the fraudulent pattern
FRAUD_TYPE_DATA = {
    "Card Probing": {"weight": 0.3, "min_transactions" : 3, "max_transactions" : 15},
    "Botting": {"weight": 0.25, "min_transactions" : 3, "max_transactions" : 15},
    "Card Cracking": {"weight": 0.2, "min_transactions" : 3, "max_transactions" : 15},
    "Account Takeover": {"weight": 0.15, "min_transactions" : 3, "max_transactions" : 15},
    "Merchant Switching": {"weight": 0.1,"min_transactions" : 3, "max_transactions" : 15},
}
