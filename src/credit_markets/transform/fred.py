# FRED Data Transformer - Bronze to Silver

from decimal import Decimal
from credit_markets.storage.postgres import PostgresClient

class FREDTransformer:
    def __init__(self):
        self.postgres = PostgresClient()
    
    def load_treasury_yields(self, data: dict, series_id: str) -> int:
        observations = data.get("observations", [])

        params_list = [
            (obs["date"], series_id, Decimal(obs["value"]))
            for obs in observations
            if obs["value"] != "."
        ]

        if not params_list:
            return 0
        
        query = """
            INSERT INTO silver.treasury_yields (observation_date, series_id, value)
            VALUES (%s, %s, %s)
            ON CONFLICT (observation_date, series_id) DO NOTHING
        """

        return self.postgres.execute_many(query, params_list)

if __name__ == "__main__":
    from credit_markets.ingestion.fred import FREDClient

    client = FREDClient()
    data = client.get_series("DGS10")

    transformer = FREDTransformer()
    rows = transformer.load_treasury_yields(data, "DGS10")
    print(f"Loaded {rows} rows into silver.treasury_yields")