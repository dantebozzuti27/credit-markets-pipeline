#fetch companies

import httpx
import sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent / 'src'))
from credit_markets.storage.postgres import PostgresClient


SEC_URL = "https://www.sec.gov/files/company_tickers.json"

def main():
    headers = {"User-Agent": "dantebozzuti@gmail.com"}
    response = httpx.get(SEC_URL, headers=headers)
    data = response.json()

    print(f"Fetched {len(data)} companies")

    rows = [
        (company["cik_str"], company["ticker"], company["title"])
        for company in data.values()
    ]    

    db = PostgresClient()
    
    query = """
        INSERT INTO reference.sec_companies (cik, ticker, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (cik) DO UPDATE SET
            ticker = EXCLUDED.ticker,
            name = EXCLUDED.name    
    """
    count = db.execute_many(query, rows)
    print(f"inserted {count} companies into reference.sec_companies")

if __name__ == "__main__":
    main()

