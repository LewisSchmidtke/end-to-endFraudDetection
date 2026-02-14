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
        currency: str
        country: str
        is_fraudulent: int
        fraud_type: str | None

        # Fraud-type specific fields -> set to None initially as they are dynamically set for each fraud type
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

    def _get_active_payment_method(self, user_id: int) -> dict:
        """
        Will fetch an active payment method from the users stored payment methods. If all methods are deactivated,
        a new payment method will be added to the users account.

        Args:
            user_id (int): The id from whom a payment method is fetched.

        Returns:
            dict: A dictionary with the necessary information about the active payment method
        """
        active_payment_method = self.DBM.fetch_active_payment_method(user_id=user_id)
        if active_payment_method is None:
            new_payment_method = self.PMG.generate_payment_method(user_id=user_id)
            self.DBM.insert_payment_method(new_payment_method)
            active_payment_method = self.DBM.fetch_active_payment_method(user_id=user_id)

        return active_payment_method

    @staticmethod
    def _determine_transaction_status(
            is_fraudulent: bool,
            fraud_type: str | None = None,
            usd_amount: float | None = None,
            payment_method: dict | None = None,
    ) -> str:
        """
        Determine if a transaction should be approved or declined. Needs the usd transaction amount and the payment
        method for non-fraudulent transactions.

        Args:
            is_fraudulent (bool): True for fraudulent transactions, False for non-fraudulent transactions.
            fraud_type (str | None): The fraud type. Needed only for fraudulent transactions.
            usd_amount (float | None): Transaction amount in USD. Needed only for non-fraudulent transactions.
            payment_method (dict | None): The payment method data. Needed only for non-fraudulent transactions.

        Returns:
            str: const.APPROVED or const.DECLINED based on decision

        Raises:
            ValueError: If fraud type is not recognized, the set approved_rate is above 1 or payment_method and
            usd_amount is None when determining the status for normal transactions
        """
        fraud_types = set(const.FRAUD_TYPE_DATA.keys())

        if is_fraudulent:
            if fraud_type not in fraud_types:
                raise ValueError(f"The status for a fraudulent transaction can not be determined for fraud type {fraud_type}")

            set_approved_rate = const.FRAUD_TYPE_DATA[fraud_type]["transaction_approved_rate"] # Get the rates from constants
            if set_approved_rate > 1:
                raise ValueError(f"Approved rate cannot be above 1, got {set_approved_rate}")
            transaction_status = random.choices(
                population=[const.DECLINED, const.APPROVED],
                weights=[1-set_approved_rate, set_approved_rate],
                k=1
            )[0]

            return transaction_status

        if payment_method is None or usd_amount is None:
            raise ValueError("Payment method and amount cannot be None for non-fraudulent transactions.")

        # Logic:
        #- High Level spending: Only approve if payment provider is renowned AND method is credit/debit card
        # - All other spending: Approve with 98% probability (2% random decline rate)
        if usd_amount >= const.TRANSACTION_CLUSTER_DATA["High Level Spending"]["min"]:
            if payment_method["payment_service_provider"] == "Renowned" and payment_method["payment_method"] in {"credit_card", "debit_card"}:
                return const.APPROVED
            return const.DECLINED

        transaction_status = random.choices(
                population=[const.DECLINED, const.APPROVED],
                weights=[0.02, 0.98],
                k=1
            )[0]

        return transaction_status



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
            population=["fraudulent", "normal"],
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

    def _generate_transaction_amount_dollar(self, transaction_context: TransactionContext) -> tuple[float, TransactionContext]:
        """
        Generates a random transaction amount in dollars. Generates it randomly, but weighted from self.transaction_cluster_weights.
        Weights and distribution functions can be set there.

        Args:
            transaction_context (TransactionContext): TransactionContext object, checks transaction_cluster attribute
            and uses the set value if possible

        Returns:
            float: A random transaction amount in dollars, rounded to two decimal places.
            TransactionContext: updated TransactionContext object

        Raises:
            ValueError: If the cluster distribution_function set in const.TRANSACTION_CLUSTER_DATA is not in {'random', 'trapezoidal', 'exp'}
        """
        # Set or randomly select a transaction cluster that will be our spending range
        if transaction_context.transaction_cluster is None:
            transaction_context.transaction_cluster = random.choices(
                self.transaction_clusters, weights=self.transaction_cluster_weights, k=1)[0]


        # Extract corresponding spending range and the distribution function
        cluster_min = self.transaction_cluster_data[transaction_context.transaction_cluster]["min"]
        cluster_max = self.transaction_cluster_data[transaction_context.transaction_cluster]["max"]
        cluster_distribution_func = self.transaction_cluster_data[transaction_context.transaction_cluster]["distribution_function"]

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

        return transaction_amount_dollar, transaction_context

    def _generate_transaction_amount_local_currency(
        self,
        transaction_context: TransactionContext,
        conversion_rates: dict | None = None,
        set_transaction_amount_dollar: float | None = None,
    ) -> tuple[float, float, TransactionContext]:
        """
        Generates a random transaction amount in a local currency. Currency can be specified or randomly generated if
        not given. If set_transaction_amount_dollar is None it calls '_generate_transaction_amount_dollar', before converting
        into the currency based on given or automatically stored conversion rates.

        Args:
            transaction_context (TransactionContext): Transaction context object.
            conversion_rates (dict | None): Raw conversion rate data from ExchangeRateAPI
            set_transaction_amount_dollar (float | None): A pre-set transaction amount in dollars

        Returns:
            float: A random transaction amount in the chosen currency, rounded to two decimal places.
            float: The generated transaction amount in usd.
            TransactionContext: Updated TransactionContext object.
        """
        # Get current conversion rates, or fallback to initial conversion rates
        active_conversion_rates = conversion_rates or self.conversion_rates

        if transaction_context.currency is None:
            # Validate currency weights and select a transaction currency
            currencies = [value["currency"] for value in const.COUNTRY_DATA.values()]
            _, currency_weights = util.unpack_weighted_dict(const.COUNTRY_DATA)
            transaction_context.currency = random.choices(currencies, weights=currency_weights, k=1)[0]

        if set_transaction_amount_dollar is None:
            dollar_amount, transaction_context = self._generate_transaction_amount_dollar(transaction_context)
        else:
            dollar_amount = set_transaction_amount_dollar # Allow for specific transaction amounts, needed for pattern generation

        # I do try and except here because the active_conversion_rate variable can be of "lean" format, see
        # CurrencyConvertor Class. Basically lean looks like this {"currency" : "rate"} and not
        # like this {"conversion_rates" : {"currency" : "rate"}}
        # TODO: Think about a function that does this.
        try:
            transaction_amount_local_currency = round(active_conversion_rates["conversion_rates"][transaction_context.currency] * dollar_amount, 2)
        except KeyError:
            transaction_amount_local_currency = round(active_conversion_rates[transaction_context.currency] * dollar_amount, 2)

        return transaction_amount_local_currency, dollar_amount, transaction_context

    @staticmethod
    def _generate_full_single_transaction_data(
            transaction_context: dataclass(),
            local_amount: float,
            usd_amount: float,
    ) -> dict:
        """
        Combines the attributes from the transaction context with the amount and currency information into a dictionary.

        Args:
            transaction_context (dataclass): TransactionContext instance with all values set.
            local_amount (float): monetary amount of the transaction in the transaction currency.
            usd_amount (float): monetary amount of the transaction in usd.
        Returns:
            dict: full transaction data inside the dict
        Raises:
            ValueError: If any attributes in transaction_context are missing
        """
        context_data = asdict(transaction_context)

        # TransactionContext needs to be validated, otherwise SQL insertion will fail
        missing = [k for k, v in context_data.items() if v is None]
        if missing:
            raise ValueError(
                f"TransactionContext has missing fields: {', '.join(missing)}"
            )

        context_data["transaction_amount_local"] = local_amount
        context_data["transaction_amount_usd"] = usd_amount
        # Convert None to string, None if the transaction is not fraudulent
        context_data["fraud_type_str"] = "None" if transaction_context.fraud_type is None else transaction_context.fraud_type

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
    ) -> list[dict]:
        """
        Generates type-specific, random transaction patterns to simulate normal or fraudulent transactions.

        Possible Fraud types:

        - Card probing: many cards with small amounts that get mostly declined
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
            List[dict]: List of transactions in the pattern. List entries are dictionaries with the relevant information
            for the specific transaction

        Raises:
            ValueError: For unrecognized fraud types.
        """
        if set_fraud_type is not None and is_fraud is not None: # Allow to explicitly create fraud pattern
            fraudulent_transaction_classifier, fraud_type = is_fraud, set_fraud_type
        else:
            fraudulent_transaction_classifier, fraud_type = self._generate_transaction_type()

        print(fraudulent_transaction_classifier, fraud_type)

        TC = self._generate_transaction_context(fraud_type=fraud_type, user_id=user_id, device_id=device_id,
                                                merchant_id=merchant_id)

        if not fraudulent_transaction_classifier:
            target_successful_transactions = random.randint(1, 3)
            successful_transactions = 0
            transaction_pattern = []
            transaction_start_time = datetime.now()

            # Fixed transaction channel as it is unlikely to buy something in the store while also buying sth online
            transaction_channel = random.choices(["Online", "Local"], [0.7, 0.3])[0]
            TC.channel = transaction_channel

            while successful_transactions < target_successful_transactions:
                transaction_delta_seconds = random.uniform(
                    const.NORMAL_TRANSACTION_DATA["min_time_seconds"],
                    const.NORMAL_TRANSACTION_DATA["max_time_seconds"]
                )
                transaction_start_time = transaction_start_time + timedelta(seconds=transaction_delta_seconds)
                TC.base_timestamp = transaction_start_time

                active_payment_method = self._get_active_payment_method(user_id=user_id)
                TC.payment_id = active_payment_method["payment_method_id"]

                local_amount, usd_amount, TC = self._generate_transaction_amount_local_currency(TC)
                TC.transaction_status = self._determine_transaction_status(False, payment_method=active_payment_method, usd_amount=usd_amount)

                if TC.transaction_status == const.DECLINED: # Set inactive
                    self.DBM.deactivate_payment_method(TC.payment_id)
                else:
                    successful_transactions += 1

                transaction_data = self._generate_full_single_transaction_data(
                    transaction_context=TC, local_amount=local_amount, usd_amount=usd_amount)

                transaction_pattern.append(transaction_data)

            return transaction_pattern

        min_transactions = const.FRAUD_TYPE_DATA[fraud_type]["min_transactions"]
        max_transactions = const.FRAUD_TYPE_DATA[fraud_type]["max_transactions"]
        number_of_transactions_in_pattern = random.randint(min_transactions, max_transactions)

        if fraud_type == "Card Probing":
            transaction_pattern = []
            transaction_start_time = datetime.now()

            for k in range(number_of_transactions_in_pattern):
                # Between 30 and 90 seconds per attempt, putting in new card numbers and pin
                # Vary in each iteration to make it more dynamics
                time_delta_seconds = random.uniform(
                    const.FRAUD_TYPE_DATA[fraud_type]["min_time_seconds"],
                    const.FRAUD_TYPE_DATA[fraud_type]["max_time_seconds"]
                )

                payment_method_info = self.PMG.generate_payment_method(user_id) # Create new payment method for user
                TC.payment_id = self.DBM.insert_payment_method(payment_method_info)

                # Choose random transaction status
                TC.transaction_status = self._determine_transaction_status(True, "Card Probing")

                if TC.transaction_status == const.DECLINED:
                    self.DBM.deactivate_payment_method(TC.payment_id)

                transaction_start_time = transaction_start_time + timedelta(seconds=time_delta_seconds)
                TC.base_timestamp = transaction_start_time

                local_currency_amount, usd_amount, TC = self._generate_transaction_amount_local_currency(
                    transaction_context=TC,
                    conversion_rates=conversion_rates
                )

                # Generate a small amount in each transaction
                transaction_data = self._generate_full_single_transaction_data(
                    transaction_context=TC, local_amount=local_currency_amount, usd_amount=usd_amount)
                transaction_pattern.append(transaction_data)

            return transaction_pattern


        elif fraud_type == "Botting":
            transaction_pattern = []
            transaction_start_time = datetime.now()

            active_payment_method = self._get_active_payment_method(user_id=user_id)
            TC.payment_id = active_payment_method["payment_method_id"]

            local_amount, usd_amount, TC = self._generate_transaction_amount_local_currency(
                transaction_context=TC, conversion_rates=conversion_rates)

            for k in range(number_of_transactions_in_pattern):
                # Between 0.5 and 1 seconds per attempt, botting repeatedly
                # Vary in each iteration to make it more dynamics
                time_delta_seconds = random.uniform(
                    const.FRAUD_TYPE_DATA[fraud_type]["min_time_seconds"],
                    const.FRAUD_TYPE_DATA[fraud_type]["max_time_seconds"]
                )

                TC.transaction_status = self._determine_transaction_status(True, "Botting")

                # I don't deactivate the payment method here when status is declined, because the declined transaction
                # is due to rate limits and not wrong payment when botting

                transaction_start_time = transaction_start_time + timedelta(seconds=time_delta_seconds)
                TC.base_timestamp = transaction_start_time

                transaction_data = self._generate_full_single_transaction_data(
                    transaction_context=TC, local_amount=local_amount, usd_amount=usd_amount
                )
                transaction_pattern.append(transaction_data)

            return transaction_pattern

        elif fraud_type == "Card Cracking":
            # TODO: Generate card_cracking pattern
            pass
            return

        elif fraud_type == "Account Takeover":
            # TODO: Generate account_takeover pattern
            pass
            return

        elif fraud_type == "Merchant Switching":
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
        Generates a TransactionContext instance and fills it with the static values for the corresponding transaction/fraud type.
        Dynamic values have to be set during the pattern generation.

        Static values (set here) for all transactions: user_id, device_id, merchant_id, currency, country, is_fraudulent and fraud_type.
        Static values (set here) for specific types: channel (for Card Probing/Botting), transaction_cluster (for Card Probing/Botting)

        Dynamic values (set later): base_timestamp, payment_id, transaction_status, (channel and transaction_cluster for other types)

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

        Raises:
            ValueError: For unrecognized fraud types.
        """
        is_fraudulent = 0 if fraud_type is None else 1
        fraud_type_str = "No Fraud" if fraud_type is None else fraud_type

        # Validate country weights and select a transaction country and fitting currency
        countries, weights = util.unpack_weighted_dict(const.COUNTRY_DATA)
        country = random.choices(countries, weights=weights, k=1)[0]
        currency = const.COUNTRY_DATA[country]["currency"]

        # Initial constants are similar in fraud and non-fraud cases.
        tc = TransactionContext(
            user_id, device_id, merchant_id, currency, country, is_fraudulent, fraud_type_str)

        # Return early here for non-fraud. Other values in tc will be set when generating the pattern
        if not is_fraudulent:
            return tc

        if fraud_type in {"Card Probing", "Botting"}:
            tc.channel = "online"

            # Probing happens at mini amounts, whereas botting happens in low tier.
            if fraud_type == "Card Probing":
                tc.transaction_cluster="Mini Level Spending"
            else:
                tc.transaction_cluster="Low Level Spending"

            return tc

        elif fraud_type == "Card Cracking":
            # TODO: Generate card_cracking pattern
            pass

        elif fraud_type == "Account Takeover":
            # TODO: Generate account_takeover pattern
            pass

        elif fraud_type == "Merchant Switching":
            # TODO: merchant_switching pattern
            pass

        else:
            raise ValueError(f"Fraud type {fraud_type_str} is not accepted.")
