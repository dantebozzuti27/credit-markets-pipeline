# FRED API Client

import httpx
from credit_markets.config.settings import get_settings

class FREDClient:
    def __init__(self):
        settings = get_settings()
        self.api_key = settings.fred_api_key.get_secret_value()
        self.base_url = "https://api.stlouisfed.org/fred"

    def get_series(self, series_id: str) -> dict:
        url = f"{self.base_url}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }

        response = httpx.get(url, params=params)
        return response.json()

if __name__ == "__main__":
    client = FREDClient()
    data = client.get_series("DGS10")
    print(data)