from .base_writer import DataWriter
import polars as pl


class ParquetWriter(DataWriter):
    REQUIRED_OPTIONS = ['compression']

    def write(self, data):
        self._ensure_directory(self.config.path)

        if isinstance(data, pl.DataFrame):
            data.write_parquet(
                self.config.path,
                compression=self.config.options['compression'],
                **self.config.options.get('polars_kwargs', {})
            )
        else:  # Spark
            (data.write
             .mode(self.config.mode)
             .option("compression", self.config.options['compression'])
             .parquet(self.config.path))