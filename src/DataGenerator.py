import random
from datetime import datetime
from faker import Faker

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
        self.country_weights = [value["weight"] for _, value in self.country_data.items()]
        self._confirm_weights(self.country_weights)

        # Define Email data and validate weights
        self.email_data = {
            "free" : {"provider" : "@free.com", "weight" : 0.7},
            "premium" : {"provider" : "@premium.com", "weight" : 0.1},
            "business" : {"provider" : "@business.com", "weight" : 0.2},
        }
        self.email_providers = list(self.email_data.keys())
        self.email_weights = [value["weight"] for _,value in self.email_data.items()]
        self._confirm_weights(self.email_weights)

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

    @staticmethod
    def _confirm_weights(weight_list):
        total_weight = sum(weight_list)
        assert total_weight == 1, f"Weighting must sum to 1, got {total_weight}"

    #TODO: # device table: device_id (unique), device_type, first_used, last_used | payment table: payment_method_id, card_provider, card_type, user_id

if __name__ == "__main__":
    user_generator = UserGenerator()
    user_generator.generate_user()