import os
from pathlib import Path
import requests
from dotenv import load_dotenv


env_path = Path(__file__).resolve().parent.parent / "credentials.env"
load_dotenv(dotenv_path=env_path)


class CurrencyDataError(Exception):
    """Exception raised when the currency API fails to provide necessary data."""
    pass


class CurrencyConvertor:
    def __init__(self):
        self._api_key = os.getenv("ExchangeRateApiKey")
        if not self._api_key:
            raise EnvironmentError("ExchangeRateApiKey not found in environment or credentials.env")

        self.base_url = f"https://v6.exchangerate-api.com/v6/{self._api_key}/latest/USD"
        self.conversion_rates = None

    def fetch_conversion_rates(self, lean=False):
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            data = response.json()
            # Lean has only conversion rates, otherwise time stamps for current exchange time, next update time, etc.
            self.conversion_rates = data["conversion_rates"] if lean else data
            return self.conversion_rates

        except requests.exceptions.RequestException as e:
            raise CurrencyDataError(f"Failed to fetch conversion rates: {e}") from e