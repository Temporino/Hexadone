import pytest
from adapters.base_adapter import DataSourceAdapter
from pandas import DataFrame


class TestBaseAdapter:
    def test_normalize_returns_dataframe(self, mocker):
        # Mock abstract methods
        class ConcreteAdapter(DataSourceAdapter):
            def connect(self): return True

            def fetch(self): return [{"id": 1}, {"id": 2}]

        adapter = ConcreteAdapter({})
        result = adapter.normalize()
        assert isinstance(result, DataFrame)
        assert len(result) == 2