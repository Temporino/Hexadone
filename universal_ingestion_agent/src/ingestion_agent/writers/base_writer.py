from abc import ABC, abstractmethod
from typing import Union
import polars as pl
from pyspark.sql import DataFrame as SparkDataFrame
from ..models.config import WriteConfig


class DataWriter(ABC):
    """Abstract base writer with common validation"""

    def __init__(self, config: WriteConfig):
        self.config = config
        self._validate_config()

    def _validate_config(self):
        """Validate writer-specific configuration"""
        required = getattr(self, "REQUIRED_OPTIONS", [])
        missing = [opt for opt in required if opt not in self.config.options]
        if missing:
            raise ValueError(f"Missing required options: {missing}")

    @abstractmethod
    def write(self, data: Union[pl.DataFrame, SparkDataFrame]) -> None:
        """Write data to destination"""
        pass

    def _ensure_directory(self, path: str) -> None:
        """Create output directory if local path"""
        if not path.startswith(('s3://', 'gs://', 'hdfs://')):
            os.makedirs(path, exist_ok=True)