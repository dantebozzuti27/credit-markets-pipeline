# Seed FRED Series Reference Data

import sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent / 'src'))
from credit_markets.storage.postgres import PostgresClient

SERIES = [
    
    # Treasury Curve
    ("DGS1MO", "1-Month Treasury", "treasury"),
    ("DGS3MO", "3-Month Treasury", "treasury"),
    ("DGS6MO", "6-Month Treasury", "treasury"),
    ("DGS1", "1-Year Treasury", "treasury"),
    ("DGS2", "2-Year Treasury", "treasury"),
    ("DGS5", "5-Year Treasury", "treasury"),
    ("DGS7", "7-Year Treasury", "treasury"),
    ("DGS10", "10-Year Treasury", "treasury"),
    ("DGS20", "20-Year Treasury", "treasury"),
    ("DGS30", "30-Year Treasury", "treasury"),
    
    # Credit Spreads
    ("BAMLC0A0CM", "ICE BofA US Corporate Index OAS", "credit_spread"),
    ("BAMLC0A4CBBB", "ICE BofA BBB US Corporate Index OAS", "credit_spread"),
    ("BAMLH0A0HYM2", "ICE BofA US High Yield Index OAS", "credit_spread"),
    
    # Policy Rates
    ("FEDFUNDS", "Federal Funds Effective Rate", "policy"),
    ("SOFR", "Secured Overnight Financing Rate", "policy"),
    
    # Risk Indicators
    ("VIXCLS", "CBOE Volatility Index", "risk"),
    ("TEDRATE", "TED Spread", "risk"),
    
    # Inflation
    ("T10YIE", "10-Year Breakeven Inflation Rate", "inflation"),
]

def main():
    db = PostgresClient()

    query = """
        INSERT INTO reference.fred_series (series_id, name, category)
        VALUES (%s, %s, %s)
        ON CONFLICT (series_id) DO UPDATE SET
            name = EXCLUDED.name,
            category = EXCLUDED.category
    """

    count = db.execute_many(query, SERIES)
    print(f"seeded {count} FRED series into reference.fred_series")

if __name__ == "__main__":
    main()