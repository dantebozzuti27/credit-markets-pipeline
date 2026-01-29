# PostgreSQL Client

import psycopg2
from contextlib import contextmanager
from credit_markets.config.settings import get_settings

class PostgresClient:
    def __init__(self):
        settings = get_settings()
        self.connection_string = (
            f"host={settings.database_host} "
            f"port={settings.database_port} "
            f"dbname={settings.database_name} "
            f"user={settings.database_user} "
            f"password={settings.database_password.get_secret_value()}"
        )
    
    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(self.connection_string)
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, query: str, params: tuple = None) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount
    
    def execute_many(self, query: str, params_list: list) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params_list)
                conn.commit()
                return cursor.rowcount

    def fetch_all(self, query: str, params: tuple = None) -> list:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

if __name__ == "__main__":
    client = PostgresClient()
    result = client.execute("SELECT 1")
    print(f"Query executed, rows: {result}")
