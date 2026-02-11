import random
from datetime import datetime
from faker import Faker
import src.utility as util
import src.constants as const


class UserGenerator:
    """
    Creates a new user.
    """
    def __init__(self):
        """
        Validates the weight distribution for country and email providers
        """
        self.FakeData = Faker()

        # Define Country data and validate weights with unpack_weighted_data
        self.countries, self.country_weights = util.unpack_weighted_dict(const.COUNTRY_DATA)

        # Define Email data and validate weights with unpack_weighted_dict
        self.email_data = const.EMAIL_DATA
        self.email_providers, self.email_provider_weights = util.unpack_weighted_dict(self.email_data)

    def generate_user(self) -> dict:
        """
        Generates a new device for a selected user. Device info consists of the device type, and a first and last use
        timestamp. During creation first and last use timestamp are similar. Last use can be updated through the function
        'update_device_use_data' in DatabaseManager.

        Returns:
            dict: user information, with keys: name, email, country, city, latitude, longitude, created_at
        """
        name = self.FakeData.name()
        country = random.choices(self.countries, weights=self.country_weights, k=1)[0]
        email_classification = random.choices(self.email_providers, weights=self.email_provider_weights, k=1)[0]
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
    """
    Creates a new device for a specific user.
    """
    def __init__(self):
        self.device_types = ["mobile","desktop","tablet"] # random.choice doesn't take a set as input

    def generate_device(self, user_id: int) -> dict:
        """
        Generates a new device for a selected user. Device info consists of the device type, and a first and last use
        timestamp. During creation first and last use timestamp are similar. Last use can be updated through the function
        'update_device_use_data' in DatabaseManager.

        Args:
            user_id (int): ID of a specific user.

        Returns:
            dict: user device information, with keys: user_id, device_type, first_used, last_used
        """
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
    """
    Creates payment methods for a specific user.
    """
    def __init__(self):
        """
        Validates the weight distribution for payment methods and payment providers.
        """
        # Define payment data and additionally validate weights in unpack_weighted_dict
        self.payment_methods, self.payment_method_weights = util.unpack_weighted_dict(const.PAYMENT_METHOD_DATA)

        # Define payment providers and validate weights in unpack_weighted_dict
        self.payment_providers, self.payment_providers_weights = util.unpack_weighted_dict(const.PAYMENT_PROVIDER_DATA)

    def generate_payment_method(self, user_id: int) -> dict:
        """
        Generates a new payment method for a selected user. Payment method consists of payment method type and the
        payment method provider
        Args:
            user_id (int): ID of a specific user.

        Returns:
            dict: Payment method information, with keys: user_id, payment_method, service_provider
        """
        payment_method_type = random.choices(self.payment_methods, weights=self.payment_method_weights, k=1)[0]
        payment_method_provider = random.choices(self.payment_providers, weights=self.payment_providers_weights, k=1)[0]

        payment_method_info = {
            "user_id" : user_id,
            "payment_method" : payment_method_type,
            "service_provider" :payment_method_provider,
        }

        return payment_method_info


class MerchantGenerator:
    """
    Creates data for a new merchant.
    """
    def __init__(self):
        """
        Validates the weight distribution for merchant type ratings.
        """
        self.FakeData = Faker()
        self.country_list = list(const.COUNTRY_DATA.keys())

        # Define merchant data and validate weights in unpack_weighted_dict
        self.merchant_rating, self.merchant_rating_weights = util.unpack_weighted_dict(const.MERCHANT_DATA)

    def generate_merchant(self) -> dict:
        """
        Generates data for a new merchant. Merchant data consists of name, rating and country.

        Returns:
            dict: Merchant information, with keys: name, rating and country
        """
        merchant_rating = random.choices(self.merchant_rating, weights=self.merchant_rating_weights, k=1)[0]
        country = random.choice(self.country_list) # No need for weighted choice here, international company distribution

        merchant_info = {
            "name" : self.FakeData.company(),
            "rating" : merchant_rating,
            "country" : country,
        }

        return merchant_info


if __name__ == "__main__":
    user_generator = UserGenerator()
    user_generator.generate_user()