"""
Pytest configuration and fixtures.

Provides:
- Environment setup for tests
- Mock clients
- Database fixtures
- S3 fixtures (using moto)
"""

import os
from datetime import date
from typing import Generator
from unittest.mock import MagicMock

import pytest

# Set test environment before importing application code
os.environ["ENVIRONMENT"] = "dev"
os.environ["FRED_API_KEY"] = "test_key"
os.environ["SEC_USER_AGENT"] = "test@example.com"
os.environ["DATABASE_HOST"] = "localhost"
os.environ["DATABASE_PASSWORD"] = "test"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"


@pytest.fixture(scope="session")
def test_date() -> date:
    """Standard test date."""
    return date(2026, 1, 27)


@pytest.fixture
def mock_fred_response() -> dict:
    """Sample FRED API response."""
    return {
        "seriess": [
            {
                "id": "DGS10",
                "title": "10-Year Treasury Constant Maturity Rate",
                "frequency": "Daily",
                "units": "Percent",
            }
        ],
        "observations": [
            {
                "date": "2026-01-27",
                "value": "4.25",
                "realtime_start": "2026-01-27",
                "realtime_end": "2026-01-27",
            },
            {
                "date": "2026-01-26",
                "value": "4.23",
                "realtime_start": "2026-01-27",
                "realtime_end": "2026-01-27",
            },
        ],
    }


@pytest.fixture
def mock_sec_response() -> dict:
    """Sample SEC EDGAR submissions response."""
    return {
        "cik": "0000320193",
        "name": "Apple Inc.",
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-24-000001"],
                "form": ["10-K"],
                "filingDate": ["2026-01-15"],
                "acceptanceDateTime": ["2026-01-15T16:30:00.000Z"],
                "primaryDocument": ["aapl-20251231.htm"],
                "primaryDocDescription": ["10-K"],
                "size": [15000000],
            }
        },
    }


@pytest.fixture
def mock_httpx_client(mock_fred_response: dict):
    """Mock httpx client for testing extractors."""
    import httpx
    from unittest.mock import patch
    
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = mock_fred_response
    mock_response.raise_for_status.return_value = None
    
    with patch("httpx.Client") as mock_client:
        mock_client.return_value.__enter__ = MagicMock(return_value=mock_client.return_value)
        mock_client.return_value.__exit__ = MagicMock(return_value=None)
        mock_client.return_value.request.return_value = mock_response
        yield mock_client


# ─────────────────────────────────────────────────────────────────────────────
# Markers
# ─────────────────────────────────────────────────────────────────────────────


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "e2e: End-to-end tests")
    config.addinivalue_line("markers", "slow: Slow tests")
