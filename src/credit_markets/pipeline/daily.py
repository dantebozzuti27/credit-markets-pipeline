# Daily Pipeline Orchestrator

from datetime import datetime, date
from credit_markets.ingestion.fred import FREDClient
from credit_markets.ingestion.sec import SECClient
from credit_markets.storage.s3 import S3Client
from credit_markets.storage.postgres import PostgresClient
from credit_markets.transform.fred import FREDTransformer
from credit_markets.transform.sec import SECTransformer
from credit_markets.observability.logging import get_logger
from credit_markets.utils.parallel import parallel_map

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

        def process_fred(series_id):
            fred_data = self.fred.get_series(series_id)
            s3_key = f"bronze/fred/{target_date}/{series_id}.json"
            self.s3.write_json(fred_data, s3_key)
            return self.fred_transformer.load_treasury_yields(fred_data, series_id)

        fred_results = parallel_map(process_fred, series_list, max_workers=5, rate_limit=5.0)
        total_fred_rows = sum(rows for _, rows in fred_results)

        results["fred"]["series"] = series_list
        results["fred"]["silver_rows"] = total_fred_rows

        company_rows = self.postgres.fetch_all(
            "SELECT cik FROM reference.sec_companies WHERE is_active = TRUE"
        )
        cik_list = [row[0] for row in company_rows]
        total_sec_rows = 0

        def process_sec(cik):
            sec_data = self.sec.get_company_filings(cik)
            sec_key = f"bronze/sec/{target_date}/{cik}.json"
            self.s3.write_json(sec_data, sec_key)
            return self.sec_transformer.load_filings(sec_data)

        sec_results = parallel_map(process_sec, cik_list, max_workers=5, rate_limit=5.0)
        total_sec_rows = sum(rows for _, rows in sec_results)
        
        results["sec"]["companies"] = len(cik_list)
        results["sec"]["silver_rows"] = total_sec_rows

        self.logger.info(f"Pipeline complete: {total_fred_rows} FRED rows, {total_sec_rows} SEC rows")

        return results

if __name__ == "__main__":
    pipeline = DailyPipeline()
    results = pipeline.run(date.today())
    print(f"Pipeline Completed: {results}")