"""
Settings module - Load configuration from environment variables.

YOUR TASK:
1. Create a Settings class using pydantic_settings.BaseSettings
2. Add fields for: fred_api_key, sec_user_agent, database_host, database_port, s3_bucket
3. Configure it to load from .env file

Hints:
- from pydantic_settings import BaseSettings, SettingsConfigDict
- from pydantic import SecretStr
- Use SecretStr for sensitive values (API keys, passwords)
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    # API Keys
    fred_api_key: SecretStr
    sec_user_agent: str
    
    # Database
    database_host: str
    database_port: int
    database_name: str
    database_user: str
    database_password: SecretStr
    
    # S3
    s3_bucket: str
    aws_region: str
    aws_endpoint_url: str | None = None 

@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

if __name__ == "__main__":
    settings = Settings()
    print(f"FRED key: {settings.fred_api_key}")
    print(f"SEC agent: {settings.sec_user_agent}")
    print(f"Database: {settings.database_host}:{settings.database_port}/{settings.database_name}")
    print(f"S3 bucket: {settings.s3_bucket}")

