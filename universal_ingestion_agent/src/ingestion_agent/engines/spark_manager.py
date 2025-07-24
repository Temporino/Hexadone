from pyspark.sql import SparkSession
from contextlib import contextmanager

class SparkManager:
    _session = None

    @classmethod
    def get_session(cls):
        if not cls._session:
            cls._session = SparkSession.builder.getOrCreate()
        return cls._session

    @classmethod
    @contextmanager
    def managed_session(cls):
        """Context manager for session lifecycle"""
        try:
            yield cls.get_session()
        finally:
            if cls._session:
                cls._session.stop()
                cls._session = None