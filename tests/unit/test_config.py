"""
Unit tests for configuration module.
"""

import os
from unittest.mock import patch

import pytest

from credit_markets.config.settings import Settings


@pytest.mark.unit
class TestSettings:
    """Tests for Settings class."""
    
    def test_loads_from_environment(self):
        """Settings should load from environment variables."""
        with patch.dict(os.environ, {
            "ENVIRONMENT": "prod",
            "FRED_API_KEY": "my-api-key",
            "SEC_USER_AGENT": "test@example.com",
            "DATABASE_HOST": "db.example.com",
            "DATABASE_PORT": "5433",
        }):
            settings = Settings()
            
            assert settings.environment == "prod"
            assert settings.fred_api_key.get_secret_value() == "my-api-key"
            assert settings.sec_user_agent == "test@example.com"
            assert settings.database_host == "db.example.com"
            assert settings.database_port == 5433
    
    def test_validates_sec_user_agent_email(self):
        """SEC user agent must be a valid email."""
        with patch.dict(os.environ, {
            "FRED_API_KEY": "test",
            "SEC_USER_AGENT": "not-an-email",
        }):
            with pytest.raises(ValueError, match="valid email"):
                Settings()
    
    def test_database_url_property(self):
        """database_url should construct valid connection string."""
        with patch.dict(os.environ, {
            "FRED_API_KEY": "test",
            "SEC_USER_AGENT": "test@example.com",
            "DATABASE_HOST": "localhost",
            "DATABASE_PORT": "5432",
            "DATABASE_NAME": "mydb",
            "DATABASE_USER": "myuser",
            "DATABASE_PASSWORD": "mypass",
        }):
            settings = Settings()
            
            assert settings.database_url == "postgresql://myuser:mypass@localhost:5432/mydb"
    
    def test_is_production_property(self):
        """is_production should return True only for prod environment."""
        with patch.dict(os.environ, {
            "ENVIRONMENT": "prod",
            "FRED_API_KEY": "test",
            "SEC_USER_AGENT": "test@example.com",
        }):
            settings = Settings()
            assert settings.is_production is True
        
        with patch.dict(os.environ, {
            "ENVIRONMENT": "dev",
            "FRED_API_KEY": "test",
            "SEC_USER_AGENT": "test@example.com",
        }):
            settings = Settings()
            assert settings.is_production is False
    
    def test_default_values(self):
        """Settings should have sensible defaults."""
        with patch.dict(os.environ, {
            "FRED_API_KEY": "test",
            "SEC_USER_AGENT": "test@example.com",
        }, clear=True):
            # Need to preserve some env vars
            os.environ.setdefault("DATABASE_PASSWORD", "test")
            settings = Settings()
            
            assert settings.environment == "dev"
            assert settings.debug is False
            assert settings.log_level == "INFO"
            assert settings.database_host == "localhost"
            assert settings.database_port == 5432
