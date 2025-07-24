from ..writers.base_writer import DataWriter
from ..writers.parquet_writer import ParquetWriter
from ..writers.csv_writer import CsvWriter
from ..writers.delta_writer import DeltaWriter
from ..writers.jdbc_writer import JdbcWriter

class WriterFactory:
    _writers = {
        'parquet': ParquetWriter,
        'csv': CsvWriter,
        'delta': DeltaWriter,
        'jdbc': JdbcWriter
    }

    @classmethod
    def create_writer(cls, config) -> DataWriter:
        writer_class = cls._writers.get(config.format)
        if not writer_class:
            raise ValueError(f"Unsupported format: {config.format}")
        return writer_class(config)