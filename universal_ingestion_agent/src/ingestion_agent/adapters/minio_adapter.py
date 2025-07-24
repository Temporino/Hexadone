# adapters/minio_adapter.py
from minio_adapter import Minio
from io import BytesIO
import pandas as pd


class MinioAdapter(DataSourceAdapter):
    def connect(self) -> bool:
        self.client = Minio(
            self.config["storage"]["endpoint"],
            access_key=self.config["credentials"]["access_key"],
            secret_key=self.config["credentials"]["secret_key"],
            secure=False
        )
        return True

    def fetch(self) -> BytesIO:
        obj = self.client.get_object(
            self.config["storage"]["bucket"],
            self.config["storage"]["path"]
        )
        return BytesIO(obj.read())

    def normalize(self) -> pd.DataFrame:
        raw = self.fetch()
        if self.config["storage"]["format"] == "parquet":
            return pd.read_parquet(raw)
        elif self.config["storage"]["format"] == "csv":
            return pd.read_csv(raw)