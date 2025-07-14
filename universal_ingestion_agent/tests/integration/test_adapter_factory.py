from adapter_factory import AdapterFactory
from adapters.rest_api_adapter import RESTAPIAdapter
import pytest

class TestAdapterFactory:
    def test_create_rest_api_adapter(self):
        config = {"type": "rest_api", "config": {"endpoint": {"base_url": "http://test.com"}}}
        adapter = AdapterFactory.create(config)
        assert isinstance(adapter, RESTAPIAdapter)

    def test_raises_for_unknown_type(self):
        with pytest.raises(ValueError):
            AdapterFactory.create({"type": "unknown", "config": {}})