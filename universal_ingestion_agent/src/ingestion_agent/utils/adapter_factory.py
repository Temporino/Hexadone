# adapter_factory.py
from importlib import import_module
from ..adapters.base_adapter import DataSourceAdapter
from typing import Dict

class AdapterFactory:
    @staticmethod
    def create(config: Dict) -> DataSourceAdapter:
        adapter_type = config["type"]
        try:
            module = import_module(f"ingestion_agent.adapters.{adapter_type}_adapter")
            adapter_class = getattr(module, f"{adapter_type.capitalize()}Adapter")
            return adapter_class(config["config"])
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Unsupported adapter type: {adapter_type}") from e