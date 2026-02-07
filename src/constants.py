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
    "Low Level Spending" : {"min" : 10, "max" : 100, "weight" : 0.9, "distribution" : "random"},
    "Mid Level Spending" : {"min" : 101, "max" : 10000, "weight" : 0.095, "distribution" : "trapezoidal"},
    "High Level Spending" : {"min" : 10001, "max" : 10000000, "weight" : 0.005, "distribution" : "exp"}
}
