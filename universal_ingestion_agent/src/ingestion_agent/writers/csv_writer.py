from .base_writer import DataWriter
import polars as pl

class CsvWriter(DataWriter):

    def write(self, data):
        if isinstance(data, pl.DataFrame):
            data.to_pandas().to_csv()
        else:
            (data.write
             .format("csv")
             .options(**self.config.options)
             .mode(self.config.mode)
             .save())