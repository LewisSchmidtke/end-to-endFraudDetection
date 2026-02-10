import random
import math
from datetime import datetime

import numpy as np

import src.utility as util
import src.constants as const


class TransactionGenerator:
    def __init__(self, conversion_rates, fraud_rate=0.01):
        # Set conversion rates
        self.conversion_rates = conversion_rates

        # Transaction classification and their likelihood
        self.fraudulent_transaction_rate = fraud_rate
        self.normal_transaction_rate = 1 - self.fraudulent_transaction_rate
        self.transaction_types = ["Fraudulent", "normal"]
        self.transaction_rates = [self.fraudulent_transaction_rate, self.normal_transaction_rate]

        # Transaction cluster amount definitions and weighting --> Values inside each cluster will have unique weighting
        self.transaction_cluster_data = const.TRANSACTION_CLUSTER_DATA
        self.transaction_clusters = list(self.transaction_cluster_data.keys())
        self.transaction_cluster_weights = [value["weight"] for value in self.transaction_cluster_data.values()]
        self.cluster_probability_distributions = [value["distribution"] for value in self.transaction_cluster_data.values()]
        util.confirm_weights(self.transaction_cluster_weights)

    def generate_transaction(self, user_id, device_id, payment_id, merchant_id, conversion_rates=None):
        transaction_amount_local_curr, currency = self._generate_transaction_amount_dollar()
        transaction_info = {
            "transaction_amount" : transaction_amount_local_curr,
            "transaction_timestamp" : datetime.now(),
            "transaction_status" : "",
            "transaction_currency" : currency,
            "transaction_country" : "",
            "transaction_channel" : "",

            "user_id": user_id,
            "merchant_id": merchant_id,
            "payment_id": payment_id,
            "device_id": device_id,
        }

        # Use this bool for machine learning ground truth later
        if self._generate_transaction_type():
            transaction_info["is_fraudulent"] = True
        else:
            transaction_info["is_fraudulent"] = False

    def generate_fraudulent_pattern(self):
        pass
        # Fraud methods:
        # Card probing: many cards with small amounts that get declined until one succeeds (different merchants perhaps)
        # Botting: not necessarily fraud, still unwanted in lots of cases. Lots of purchases in a small timeframe with same payment method
        # Retry: Try of the same item purchase with variations in card number/pin
        # Account takeover: Long inactive period with sudden purchase frequency increase maybe new location and payment method.
        # Merchant switching: payment method used for multiple categories normally, then switches to new merchant category + location, i.e. gift cards

    def _generate_transaction_type(self):
        transaction_type = random.choices(self.transaction_types, weights=self.transaction_rates, k=1)[0]
        fraudulent_bool = 0 if transaction_type == "normal" else 1
        return fraudulent_bool

    def _generate_transaction_amount_dollar(self, conversion_rates=None):
        """All values in dollar, will convert to transaction currency in other function"""
        # Get current conversion rates, or fallback to initial conversion rates
        active_conversion_rates = conversion_rates or self.conversion_rates

        # Select transaction currency
        currencies = [value["currency"] for value in const.COUNTRY_DATA.values()]
        currency_weights = [value["weight"] for value in const.COUNTRY_DATA.values()]
        util.confirm_weights(currency_weights)
        transaction_currency = random.choices(currencies, weights=currency_weights, k=1)[0]

        transaction_cluster = random.choices(self.transaction_clusters, weights=self.transaction_cluster_weights, k=1)[0]
        cluster_min = self.transaction_cluster_data[transaction_cluster]["min"]
        cluster_max = self.transaction_cluster_data[transaction_cluster]["max"]
        cluster_distribution = self.transaction_cluster_data[transaction_cluster]["distribution"]

        # For low level spending all values have same probability, no real need for a prob. dist. with low amounts
        if cluster_distribution == "random":
            transaction_amount_dollar = round(random.uniform(cluster_min, cluster_max), 2)

        # Trapezoidal distribution for mid level spending, most amounts around max-min / 2,
        elif cluster_distribution == "trapezoidal":
            floor_ratio = 0.2 # floor value 1/floor_ratio (5x) times more likely than ceil.
            u = random.random()
            sample_scaled = (1 - math.sqrt(1 - u * (1 - floor_ratio ** 2))) / (1 - floor_ratio)
            transaction_amount_dollar = round(cluster_min + (cluster_max - cluster_min) * sample_scaled, 2)

        elif cluster_distribution == "exp":
            # 1% probability at 5 mil --> calculate scale: P(x) = exp[-(x-min)/scale]
            # scale = -(x-min) / ln(P(x)) =(approx) 1,083,564
            scale = 1083564
            transaction_amount_dollar = round(min((np.random.exponential(scale=scale) + cluster_min), cluster_max), 2)

        else:
            raise ValueError(f"{cluster_distribution} is an invalid distribution. "
                             f"Only {self.cluster_probability_distributions} are available!")

        transaction_amount_local_currency = round(active_conversion_rates["conversion_rates"][transaction_currency] * transaction_amount_dollar, 2)

        return transaction_amount_local_currency, transaction_currency
