import random
import math
from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass, asdict

import numpy as np

import src.utility as util
import src.constants as const

from src.DataGenerator import PaymentMethodGenerator
from src.DatabaseManager import DatabaseManager


@dataclass
class TransactionContext:
        user_id: int
        device_id: int
        merchant_id: int
        is_fraudulent: int
        fraud_type: str | None

        # Fraud-type specific fields -> set to None initially as they are dynamically set for each fraud type
        currency: str | None = None
        country: str | None = None
        channel: str | None = None
        transaction_cluster: str | None = None
        payment_id: int | None = None
        transaction_status: str | None = None
        base_timestamp: datetime | None = None


class TransactionGenerator:
    def __init__(self, conversion_rates, fraud_rate=0.01):
        # Set conversion rates
        self.conversion_rates = conversion_rates

        # Transaction classification and their likelihood
        self.fraud_rate = fraud_rate if fraud_rate <= 1 else 0.01 # Make sure we have less than 100% fraud

        # Transaction cluster amount definitions and weighting --> Values inside each cluster will have unique weighting
        self.transaction_cluster_data = const.TRANSACTION_CLUSTER_DATA
        self.transaction_clusters, self.transaction_cluster_weights = util.unpack_weighted_dict(self.transaction_cluster_data)
        self.cluster_probability_distributions = [value["distribution_function"] for value in self.transaction_cluster_data.values()]

        self.PMG = PaymentMethodGenerator()
        self.DBM = DatabaseManager()

    def _generate_transaction_type(self) -> tuple[int, str | None]:
        """
        Chooses a random, but weighted transaction type (fraudulent or normal). Fraud weight is set with 'fraud_rate' in init.
        Afterward, chooses a random but weighted fraud type (if applicable).

        Returns:
            int : 0 if a normal transaction was selected, 1 if it is fraudulent.
            str : Type of fraud, if fraud has been selected, else None
        """
        # Choose transaction type first
        transaction_type = random.choices(
            population=["fraudulent", "normal"], # I didn't make the options 1 and 0 so its more readable
            weights=[self.fraud_rate, 1 - self.fraud_rate],
            k=1
        )[0]
        fraudulent_indicator = 0 if transaction_type == "normal" else 1

        # Choose fraud type (if applicable) second
        if fraudulent_indicator:
            fraud_types, fraud_weights = util.unpack_weighted_dict(const.FRAUD_TYPE_DATA)
            fraud_type = random.choices(
                population=fraud_types,
                weights=fraud_weights,
                k=1
            )[0]
        else:
            fraud_type = None

        return fraudulent_indicator, fraud_type

    def _generate_transaction_amount_dollar(self, set_transaction_cluster: str | None = None) -> float:
        """
        Generates a random transaction amount in dollars. Generates it randomly, but weighted from self.transaction_cluster_weights.
        Weights and distribution functions can be set there.

        Args:
            set_transaction_cluster (str | None): Can be specified to select from which transaction cluster a
            transaction amount is generated from.
        Returns:
            float: A random transaction amount in dollars, rounded to two decimal places.
        """
        # Set or randomly select a transaction cluster that will be our spending range
        if set_transaction_cluster is None:
            transaction_cluster = random.choices(
                self.transaction_clusters, weights=self.transaction_cluster_weights, k=1)[0]
        else:
            transaction_cluster = set_transaction_cluster

        # Extract corresponding spending range and the distribution function
        cluster_min = self.transaction_cluster_data[transaction_cluster]["min"]
        cluster_max = self.transaction_cluster_data[transaction_cluster]["max"]
        cluster_distribution_func = self.transaction_cluster_data[transaction_cluster]["distribution_function"]

        # For low and mini level spending all values have same probability, no real need for a prob. dist. with low amounts
        if cluster_distribution_func == "random":
            transaction_amount_dollar = round(random.uniform(cluster_min, cluster_max), 2)

        # Trapezoidal distribution for mid level spending, most amounts around max-min / 2,
        elif cluster_distribution_func == "trapezoidal":
            floor_ratio = 0.2  # floor value 1/floor_ratio (5x) times more likely than ceil.
            u = random.random()
            sample_scaled = (1 - math.sqrt(1 - u * (1 - floor_ratio ** 2))) / (1 - floor_ratio)
            transaction_amount_dollar = round(cluster_min + (cluster_max - cluster_min) * sample_scaled, 2)

        elif cluster_distribution_func == "exp":
            # 1% probability at 5 mil --> calculate scale: P(x) = exp[-(x-min)/scale]
            # scale = -(x-min) / ln(P(x)) =(approx) 1,083,564
            scale = 1083564
            transaction_amount_dollar = round(min((np.random.exponential(scale=scale) + cluster_min), cluster_max), 2)

        else:
            raise ValueError(f"{cluster_distribution_func} is an invalid distribution function. "
                             f"Only {self.cluster_probability_distributions} are available!")

        return transaction_amount_dollar

    def _generate_transaction_amount_local_currency(
        self,
        conversion_rates: dict | None = None,
        set_transaction_amount_dollar: float | None = None,
        set_transaction_cluster: str | None= None,
        set_currency: str | None = None,
    ) -> tuple[float, str]:
        """
        Generates a random transaction amount in a local currency. Currency can be specified or randomly generated if
        not given. If set_transaction_amount_dollar is None it calls '_generate_transaction_amount_dollar', before converting
        into the currency based on given or automatically stored conversion rates.
        Args:
            conversion_rates (dict | None): Raw conversion rate data from ExchangeRateAPI
            set_transaction_amount_dollar (float | None): A pre-set transaction amount in dollars
            set_transaction_cluster (str | None): A specified transaction cluster.
            set_currency (str | None): A specified currency. Has to be official abbreviation of the currency.

        Returns:
            float: A random transaction amount in the chosen currency, rounded to two decimal places.
            str: The selected currency
        """
        # Get current conversion rates, or fallback to initial conversion rates
        active_conversion_rates = conversion_rates or self.conversion_rates

        if set_currency is None:
            # Validate currency weights and select a transaction currency
            currencies = [value["currency"] for value in const.COUNTRY_DATA.values()]
            _, currency_weights = util.unpack_weighted_dict(const.COUNTRY_DATA.values())
            transaction_currency = random.choices(currencies, weights=currency_weights, k=1)[0]
        else:
            transaction_currency = set_currency

        if set_transaction_amount_dollar is None:
            dollar_amount = self._generate_transaction_amount_dollar(set_transaction_cluster)
        else:
            dollar_amount = set_transaction_amount_dollar # Allow for specific transaction amounts, needed for pattern generation

        # I do try and except here because the active_conversion_rate variable can be of "lean" format, see
        # CurrencyConvertor Class. Basically lean looks like this {"currency" : "rate"} and not
        # like this {"conversion_rates" : {"currency" : "rate"}}
        # TODO: Think about a function that does this.
        try:
            transaction_amount_local_currency = round(active_conversion_rates["conversion_rates"][transaction_currency] * dollar_amount, 2)
        except KeyError:
            transaction_amount_local_currency = round(active_conversion_rates[transaction_currency] * dollar_amount, 2)

        return transaction_amount_local_currency, transaction_currency

    @staticmethod
    def _generate_full_single_transaction_data(
            transaction_context: dataclass(),
            monetary_amount: float,
            currency: str,
    ):
        """
        Combines the attributes from the transaction context with the amount and currency information into a dictionary.

        Args:
            transaction_context (dataclass): TransactionContext instance with all values set.
            monetary_amount (float): monetary amount of the transaction.
            currency (str): Currency of the transaction context.
        Returns:
            dict: full transaction data inside the dict
        """
        context_data = asdict(transaction_context)

        # TransactionContext needs to be validated, otherwise SQL insertion will fail
        missing = [k for k, v in context_data.items() if v is None]
        if missing:
            raise ValueError(
                f"TransactionContext has missing fields: {', '.join(missing)}"
            )

        context_data["transaction_amount"] = monetary_amount
        context_data["transaction_currency"] = currency

        return context_data


    # UNFINISHED FUNCTIONS ------------------------------------------------

    def generate_transaction_pattern(
        self,
        user_id: int,
        device_id: int,
        merchant_id: int,
        is_fraud: int | None = None,
        set_fraud_type: str | None = None,
        conversion_rates: dict | None = None,
    ):
        """
        Generates type-specific, random transaction patterns to simulate normal or fraudulent transactions.

        Possible Fraud types:

        - Card probing: many cards with small amounts that get declined until one succeeds (different merchants perhaps)
        - Botting: not necessarily fraud, still unwanted in lots of cases. Lots of purchases in a small timeframe with same payment method
        - Retry: Try of the same item purchase with variations in card number/pin
        - Account takeover: Long inactive period with sudden purchase frequency increase maybe new location and payment method.
        - Merchant switching: payment method used for multiple categories normally, then switches to new merchant category + location, i.e. gift cards

        Args:
            user_id (int): User ID for whom the pattern will be generated
            device_id (int): Device ID from a user which is linked to the pattern
            merchant_id (int): Merchant ID that is associated with the pattern
            is_fraud (int | None): 0/1 to set if a pattern should be fraudulent or not
            set_fraud_type (str | None): Specific fraud type for which a pattern should be generated.
            Has to be set in combination with is_fraud = 1. That combination forces a pattern of that type to be generated.
            conversion_rates (dict | None): Current conversion rates in dict format. Can be left empty,
            as initial conversion rates are set in class attribute.

        Returns:

        """
        if set_fraud_type is not None and is_fraud is not None: # Allow to explicitly create fraud pattern
            fraudulent_transaction_classifier, fraud_type = is_fraud, set_fraud_type
        else:
            fraudulent_transaction_classifier, fraud_type = self._generate_transaction_type()

        TC = self._generate_transaction_context(fraud_type=fraud_type, user_id=user_id, device_id=device_id,
                                                merchant_id=merchant_id)

        if not fraudulent_transaction_classifier:
            # TODO: Generate normal transaction pattern here and return the data
            pass
            return

        min_transactions = const.FRAUD_TYPE_DATA[fraud_type]["min_transactions"]
        max_transactions = const.FRAUD_TYPE_DATA[fraud_type]["max_transactions"]
        number_of_transactions_in_pattern = random.randint(min_transactions, max_transactions)
        fraud_type = fraud_type.lower().replace(" ", "_")

        if fraud_type == "card_probing":
            transaction_pattern = []
            pattern_start_time = datetime.now()

            for k in range(number_of_transactions_in_pattern):
                # Between 30 and 90 seconds per attempt, putting in new card numbers and pin
                # Vary in each iteration to make it more dynamics
                time_delta_seconds = random.uniform(30, 90)

                payment_method_info = self.PMG.generate_payment_method(user_id) # Create new payment method for user
                payment_method_id = self.DBM.insert_payment_method(payment_method_info)
                TC.payment_id = payment_method_id

                # Choose random transaction status
                TC.transaction_status = random.choices(
                    population=["Declined", "Approved"],
                    weights=[0.9, 0.1],
                    k=1
                )[0]
                TC.base_timestamp = pattern_start_time + timedelta(seconds=k * time_delta_seconds) # Add dynamic delta

                local_currency_amount, currency = self._generate_transaction_amount_local_currency(
                    set_transaction_cluster=TC.transaction_cluster,
                    set_currency=TC.currency,
                    conversion_rates=conversion_rates
                )

                #generate a small amount eac
                transaction_data = self._generate_full_single_transaction_data(
                    transaction_context=TC, amount=local_currency_amount, currency=currency)
                transaction_pattern.append(transaction_data)

            return transaction_pattern


        elif fraud_type == "botting":
            transaction_pattern = []
            pattern_start_time = datetime.now()
            # TODO: Extract payment method for user id from DB
            payment_method_id = self.DBM.get_payment_method(user_id) # Method to be build
            TC.payment_id = payment_method_id

            payment_amount = self._generate_transaction_amount_local_currency(set_transaction_cluster=TC.transaction_cluster)

            for k in range(number_of_transactions_in_pattern):
                # Between 0.5 and 1 seconds per attempt, botting repeatedly
                # Vary in each iteration to make it more dynamics
                time_delta_seconds = random.uniform(0.5, 1)

                # Choose random transaction status
                TC.transaction_status = random.choices(
                    population=["Declined", "Approved"],
                    weights=[0.1, 0.9], # I set a fictional 10% decline rate due to factors like rate limits when scalping
                    k=1
                )[0]
                TC.base_timestamp = pattern_start_time + timedelta(seconds=k * time_delta_seconds)  # Add dynamic delta

                transaction_data = self._generate_full_single_transaction_data()
                transaction_pattern.append(transaction_data)

            return transaction_pattern

        elif fraud_type == "card_cracking":
            # TODO: Generate card_cracking pattern
            pass
            return

        elif fraud_type == "account_takeover":
            # TODO: Generate account_takeover pattern
            pass
            return

        elif fraud_type == "merchant_switching":
            # TODO: merchant_switching pattern
            pass
            return

        else:
            raise ValueError(f"Fraud type {fraud_type} is not supported, See keys in FRAUD_TYPE_DATA for possible values.")

    def _generate_transaction_context(
            self,
            fraud_type: str | None,
            user_id: int,
            device_id: int,
            merchant_id: int,
            payment_id: int | None = None,
    ) -> TransactionContext:
        """
        Generates a TransactionContext instance and fills it with the static values for the selected fraud type.
        Dynamic values have to be set during the pattern generation.

        Args:
            fraud_type (str): The type of fraud for which the context should be generated. Important to differentiate
            between static and dynamic attributes.
            user_id (int): User id which is associated with the transaction context.
            device_id (int): Device id which is associated with the transaction context.
            merchant_id (int): Merchant id which is associated with the transaction context.
            payment_id (int | None): Optional payment id which is associated with the transaction context.
            Optional because it has to be dynamic for certain fraud types.

        Returns:
            TransactionContext : TransactionContext instance with static values inserted.
        """
        is_fraudulent = 1 if fraud_type is not None else 0
        fraud_type_value = fraud_type if fraud_type is not None else "No Fraud"
        fraud_type = fraud_type.lower().replace(" ", "_")

        tc = TransactionContext(user_id, device_id, merchant_id, is_fraudulent, fraud_type_value)

        # Return early here for non-fraud. Other values in tc will be set when generating the pattern
        if not is_fraudulent:
            return tc


        if fraud_type in {"card_probing", "botting"}:
            # Initial constants are similar in both cases.
            # Validate country weights and select a transaction country and fitting currency
            countries, weights = util.unpack_weighted_dict(const.COUNTRY_DATA)
            country = random.choices(countries, weights=weights, k=1)[0]
            currency = const.COUNTRY_DATA[country]["currency"]

            tc.country = country
            tc.currency = currency
            tc.channel = "online"

            # Probing happens at mini amounts, whereas botting happens in low tier.
            if fraud_type == "card_probing":
                tc.transaction_cluster="Mini Level Spending"
            else:
                tc.transaction_cluster="Low Level Spending"

            return tc

        elif fraud_type == "card_cracking":
            # TODO: Generate card_cracking pattern
            pass

        elif fraud_type == "account_takeover":
            # TODO: Generate account_takeover pattern
            pass

        elif fraud_type == "merchant_switching":
            # TODO: merchant_switching pattern
            pass

        else:
            raise ValueError(f"Fraud type {fraud_type} is not accepted.")


if __name__ == "__main__":
    from CurrencyConvertor import CurrencyConvertor
    CC = CurrencyConvertor()
    rates = CC.fetch_conversion_rates()
    TG = TransactionGenerator(rates)

    data = TG.generate_transaction_pattern(1,"Card Probing", 1, 1, 1)
    amount = [x["transaction_amount"] for x in data]
    status = [x["transaction_status"] for x in data]
