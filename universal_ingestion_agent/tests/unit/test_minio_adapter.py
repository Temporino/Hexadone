from adapters.minio_adapter import MinIOAdapter
from unittest.mock import patch, MagicMock
import pandas as pd

class TestMinIOAdapter:
    @patch('minio.Minio')
    def test_connect_initializes_client(self, mock_minio):
        adapter = MinIOAdapter({
            "storage": {"bucket": "test", "path": "file.parquet", "format": "parquet"},
            "credentials": {"endpoint": "localhost", "access_key": "key", "secret_key": "secret"}
        })
        adapter.connect()
        assert adapter.client is not None

    @patch('minio.Minio.get_object')
    def test_normalize_returns_dataframe(self, mock_get_object):
        mock_get_object.return_value = MagicMock(read=MagicMock(return_value=b'parquet_data'))
        with patch('pandas.read_parquet', return_value=pd.DataFrame({"col": [1, 2]})) as mock_read:
            adapter = MinIOAdapter({"storage": {"format": "parquet"}, "credentials": {}})
            df = adapter.normalize()
            assert not df.empty