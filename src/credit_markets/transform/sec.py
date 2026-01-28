#SEC Data Transformer - Bronze to Silver

from credit_markets.storage.postgres import PostgresClient

class SECTransformer:
    def __init__(self):
        self.postgres = PostgresClient()
    
    def load_filings(self, data: dict) -> int:
        cik = data.get("cik", "")
        company_name = data.get("name", "")
        recent = data.get("filings", {}).get("recent", {})
        accession_numbers = recent.get("accessionNumber", [])
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        rows_inserted = 0
        for i in range(len(accession_numbers)):
            query = """
                INSERT INTO silver.sec_filings
                    (accession_number, cik, company_name, filing_type, filing_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (accession_number) DO NOTHING
            """
            params = (
                accession_numbers[i],
                cik,
                company_name,
                forms[i],
                filing_dates[i]
            )
            rows_inserted += self.postgres.execute(query, params)
        return rows_inserted
    
if __name__ == "__main__":
    from credit_markets.ingestion.sec import SECClient

    client = SECClient()
    data = client.get_company_filings("320193")

    transformer = SECTransformer()
    rows = transformer.load_filings(data)
    print(f"Loaded {rows} filings into silver.sec_filings")