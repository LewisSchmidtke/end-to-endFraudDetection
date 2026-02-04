import random
from datetime import datetime
from faker import Faker
import src.utility as util


class UserGenerator:
    def __init__(self):
        self.FakeData = Faker()

        # Define Country data and validate weights
        self.country_data = {
            "US" : {"currency" : "USD", "weight" : 0.5},
            "CN" : {"currency" : "CNY", "weight" : 0.15},
            "IN" : {"currency" : "INR", "weight" : 0.05},
            "CA" : {"currency" : "CAD", "weight" : 0.025},
            "JP" : {"currency" : "JPY", "weight" : 0.025},
            "DE" : {"currency" : "EUR", "weight" : 0.075},
            "GB" : {"currency" : "GBP", "weight" : 0.1},
            "FR" : {"currency" : "EUR", "weight" : 0.075}
        }
        self.countries = list(self.country_data.keys())
        self.country_weights = [value["weight"] for value in self.country_data.values()]
        util.confirm_weights(self.country_weights)

        # Define Email data and validate weights
        self.email_data = {
            "free" : {"provider" : "@free.com", "weight" : 0.7},
            "premium" : {"provider" : "@premium.com", "weight" : 0.1},
            "business" : {"provider" : "@business.com", "weight" : 0.2},
        }
        self.email_providers = list(self.email_data.keys())
        self.email_weights = [value["weight"] for value in self.email_data.values()]
        util.confirm_weights(self.email_weights)

    def generate_user(self):
        name = self.FakeData.name()
        country = random.choices(self.countries, weights=self.country_weights, k=1)[0]
        email_classification = random.choices(self.email_providers, weights=self.email_weights, k=1)[0]
        user_email = name.replace(" ", ".").lower() + self.email_data[email_classification]["provider"] # create fake email
        lat, lon, city, _, _ = self.FakeData.local_latlng(country_code=country) # get city, latitude and longitude for country

        main_user_info = {
            "name" : name,
            "email": user_email,
            "country" : country,
            "city" : city,
            "latitude" : lat,
            "longitude" : lon,
            "created_at" : datetime.now(),
        }

        return main_user_info


class DeviceGenerator:
    def __init__(self):
        self.device_types = ["mobile","desktop","tablet"] # random.choice doesn't take a set as input

    def generate_device(self, user_id):
        # At creation first and last used are similar, first used will keep this value,
        # whereas last used can be updated by a different function
        first_used = last_used = datetime.now()
        device_type = random.choice(self.device_types) # Choose random device type, currently no weights needed

        user_device_info = {
            "user_id" : user_id,
            "device_type" : device_type,
            "first_used" : first_used,
            "last_used" : last_used,
        }

        return user_device_info


class PaymentMethodGenerator:
    def __init__(self):
        # Define payment data and validate weights
        self.payment_method_data = {
            "bank_transfer" : {"weight": 0.23},
            "credit_card" : {"weight": 0.30},
            "debit_card" : {"weight" : 0.36},
            "BNPL" : {"weight" : 0.1},
            "crypto_currency" : {"weight" : 0.01},
        }
        self.payment_methods = list(self.payment_method_data.keys())
        self.payment_method_weights = [value["weight"] for value in self.payment_method_data.values()]
        util.confirm_weights(self.payment_method_weights)

        # Define payment providers and validate weights
        # We use this classification to further calculate the risk score later
        self.payment_provider_data = {
            "Renowned" : {"weight": 0.75},
            "Mid" : {"weight": 0.24},
            "Unknown" : {"weight": 0.01},
        }
        self.payment_providers = list(self.payment_provider_data.keys())
        self.payment_providers_weights = [value["weight"] for value in self.payment_provider_data.values()]
        util.confirm_weights(self.payment_providers_weights)

    def generate_payment_method(self, user_id):
        payment_method_type = random.choices(self.payment_methods, weights=self.payment_method_weights, k=1)[0]
        payment_method_provider = random.choices(self.payment_providers, weights=self.payment_providers_weights, k=1)[0]

        payment_method_info = {
            "user_id" : user_id,
            "payment_method" : payment_method_type,
            "service_provider" :payment_method_provider,
        }

        return payment_method_info


if __name__ == "__main__":
    user_generator = UserGenerator()
    user_generator.generate_user()