import random
import math
from datetime import datetime

import numpy as np

import src.utility as util


class TransactionGenerator:
    def __init__(self, user_id, device_id, payment_id, merchant_id, fraud_rate=0.01):
        # Transaction classification and their likelihood
        self.fraudulent_transaction_rate = fraud_rate
        self.normal_transaction_rate = 1 - self.fraudulent_transaction_rate
        self.transaction_types = ["Fraudulent", "normal"]
        self.transaction_rates = [self.fraudulent_transaction_rate, self.normal_transaction_rate]


        # Transaction cluster amount definitions and weighting --> Values inside each cluster will have unique weighting
        self.transaction_cluster_data = {
            "Low Level Spending" : {"min" : 10, "max" : 100, "weight" : 0.9, "distribution" : "random"},
            "Mid Level Spending" : {"min" : 101, "max" : 10000, "weight" : 0.095, "distribution" : "trapezoidal"},
            "High Level Spending" : {"min" : 10001, "max" : 10000000, "weight" : 0.005, "distribution" : "exp"}
        }
        self.transaction_clusters = list(self.transaction_cluster_data.keys())
        self.transaction_cluster_weights = [value["weight"] for value in self.transaction_cluster_data.values()]
        self.cluster_probability_distributions = [value["distribution"] for value in self.transaction_cluster_data.values()]
        util.confirm_weights(self.transaction_cluster_weights)



    def generate_transaction(self):
        pass
        # transaction_info = {
        #     "transaction_amount" : "",
        #     "transaction_timestamp" : datetime.now(),
        #     "transaction_status" : "",
        #     "transaction" : ""
        # }

    def _generate_transaction_type(self):
        transaction_type = random.choices(self.transaction_types, weights=self.transaction_rates, k=1)[0]
        fraudulent_bool = 0 if transaction_type == "normal" else 1
        return fraudulent_bool


    def _generate_transaction_amount_dollar(self):
        """All values in dollar, will convert to transaction currency in other function"""
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

        # TODO: we need to choose a local currency from currency data during user generation. Then find a way to either
        #  give the conversion rates to transaction generator or call the currency converter inside here. Convert to local currency, then return.

        return transaction_amount_dollar
