# Daily Pipeline Orchestrator

from datetime import datetime, date
from credit_markets.ingestion.fred import FREDClient
from credit_markets.ingestion.sec import SECClient
from credit_markets.storage.s3 import S3Client
from credit_markets.storage.postgres import PostgresClient
from credit_markets.transform.fred import FREDTransformer
from credit_markets.transform.sec import SECTransformer
from credit_markets.observability.logging import get_logger

class DailyPipeline:
    def __init__(self):
        self.logger = get_logger("credit_markets.pipeline")
        self.fred = FREDClient()
        self.sec = SECClient()
        self.s3 = S3Client()
        self.postgres = PostgresClient()
        self.fred_transformer = FREDTransformer()
        self.sec_transformer = SECTransformer()

    def run(self, target_date: date) -> dict:
        results = {"date": str(target_date), "fred": {}, "sec": {}}
        self.logger.info(f"Starting pipeline for {target_date}")

        rows = self.postgres.fetch_all(
            "SELECT series_id FROM reference.fred_series WHERE is_active = TRUE"
        )
        series_list = [row[0] for row in rows]
        total_fred_rows = 0

        for series_id in series_list:
            fred_data = self.fred.get_series(series_id)
            s3_key = f"bronze/fred/{target_date}/{series_id}.json"
            self.s3.write_json(fred_data, s3_key)
            loaded_rows = self.fred_transformer.load_treasury_yields(fred_data, series_id)
            total_fred_rows += loaded_rows

        results["fred"]["series"] = series_list
        results["fred"]["silver_rows"] = total_fred_rows

        company_rows = self.postgres.fetch_all(
            "SELECT cik FROM reference.sec_companies WHERE is_active = TRUE"
        )
        cik_list = [row[0] for row in company_rows]
        total_sec_rows = 0

        for cik in cik_list:
            sec_data = self.sec.get_company_filings(cik)
            sec_key = f"bronze/sec/{target_date}/{cik}.json"
            self.s3.write_json(sec_data, sec_key)
            sec_rows = self.sec_transformer.load_filings(sec_data)
            total_sec_rows += sec_rows
        
        results["sec"]["companies"] = len(cik_list)
        results["sec"]["silver_rows"] = total_sec_rows

        self.logger.info(f"Pipeline complete: {total_fred_rows} FRED rows, {total_sec_rows} SEC rows")

        return results

if __name__ == "__main__":
    pipeline = DailyPipeline()
    results = pipeline.run(date.today())
    print(f"Pipeline Completed: {results}")