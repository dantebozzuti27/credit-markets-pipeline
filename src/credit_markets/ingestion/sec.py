# SEC EDGAR Client

import httpx
from credit_markets.config.settings import get_settings

class SECClient:
    def __init__(self):
        settings = get_settings()
        self.user_agent = settings.sec_user_agent
        self.base_url = "https://data.sec.gov"
        self.headers = {"User-Agent": self.user_agent}

    def get_company_filings(self, cik: str) -> dict:
        cik_padded = cik.zfill(10)
        url = f"{self.base_url}/submissions/CIK{cik_padded}.json"
        response = httpx.get(url, headers=self.headers)
        return response.json()

if __name__ == "__main__":
    client = SECClient()
    data = client.get_company_filings("320193")
    print(f"Company: {data.get('name')}")
    print(f"Recent filings: {len(data.get('filings', {}).get('recent', {}).get('form'))}")