import pytest
from unittest.mock import MagicMock
import pandas as pd
import json
from pathlib import Path

# ----- Fixture: Sample Configurations -----
@pytest.fixture
def rest_api_config():
    """Config for RESTAPIAdapter"""
    return {
        "type": "api",
        "config": {
            "auth": {"type": "oauth2", "token_url": "http://auth.test", "credentials": {"client_id": "test"}},
            "endpoint": {"base_url": "http://api.test", "path": "/orders"}
        }
    }

@pytest.fixture
def minio_config():
    """Config for MinIOAdapter"""
    return {
        "type": "minio",
        "config": {
            "storage": {"bucket": "test-bucket", "path": "data.parquet", "format": "parquet"},
            "credentials": {"endpoint": "localhost:9000", "access_key": "minio", "secret_key": "minio123"}
        }
    }

# ----- Fixture: Mock Data -----
@pytest.fixture
def mock_api_response():
    """Mock API response data"""
    with open(Path(__file__).parent / "fixtures/sample_api_response.json") as f:
        return json.load(f)

@pytest.fixture
def mock_parquet_data():
    """Mock Parquet file data"""
    return pd.read_parquet(Path(__file__).parent / "fixtures/sample_data.parquet")

# ----- Fixture: Mock Clients -----
@pytest.fixture
def mock_minio_client():
    """Mock MinIO client"""
    client = MagicMock()
    client.get_object.return_value = MagicMock(read=MagicMock(return_value=b"mock_data"))
    return client

@pytest.fixture
def mock_requests_session():
    """Mock requests.Session"""
    session = MagicMock()
    session.get.return_value.json.return_value = {"orders": [{"id": 1}]}
    return session