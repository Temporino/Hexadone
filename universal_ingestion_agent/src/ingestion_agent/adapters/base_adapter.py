# adapters/base_adapter.py
from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd


class DataSourceAdapter(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source"""
        pass

    @abstractmethod
    def fetch(self) -> Any:
        """Fetch raw data from the source"""
        pass

    def normalize(self) -> pd.DataFrame:
        """Convert raw data to DataFrame with schema validation"""
        raw_data = self.fetch()
        return pd.DataFrame(raw_data)