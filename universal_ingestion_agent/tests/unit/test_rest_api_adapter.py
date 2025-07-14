import pytest
from adapters.rest_api_adapter import RESTAPIAdapter
from unittest.mock import patch, Mock

class TestRESTAPIAdapter:
    @patch('requests.Session')
    def test_connect_sets_auth_header(self, mock_session):
        config = {
            "auth": {"type": "oauth2", "token_url": "http://test.com", "credentials": {"client_id": "id"}},
            "endpoint": {"base_url": "http://api.com"}
        }
        adapter = RESTAPIAdapter(config)
        adapter.connect()
        assert "Authorization" in adapter.session.headers

    @patch('requests.Session.get')
    def test_fetch_returns_data(self, mock_get):
        mock_get.return_value.json.return_value = {"orders": [{"id": 1}]}
        adapter = RESTAPIAdapter({"endpoint": {"base_url": "http://api.com", "path": "/orders"}})
        data = adapter.fetch()
        assert data == [{"id": 1}]