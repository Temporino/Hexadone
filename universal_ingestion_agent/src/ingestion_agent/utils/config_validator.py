# config_validator.py
from pydantic import BaseModel, ValidationError
from typing import Optional
from typing import Dict


class RESTAPIConfig(BaseModel):
    auth: dict
    endpoint: dict
    schema_inference: Optional[dict]

class ConfigValidator:
    @staticmethod
    def validate(config: Dict, adapter_type: str) -> bool:
        try:
            if adapter_type == "api":
                RESTAPIConfig(**config)
            # TODO Add other config models (MinIO, PostgreSQL, etc.)
            return True
        except ValidationError as e:
            raise ValueError(f"Invalid config: {e}")