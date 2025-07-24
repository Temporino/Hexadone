from .base_writer import DataWriter
import polars as pl

class JdbcWriter(DataWriter):
    REQUIRED_OPTIONS = ['url', 'driver']

    def write(self, data):
        if isinstance(data, pl.DataFrame):
            data.to_pandas().to_sql(
                self.config.table,
                self.config.options['url'],
                if_exists=self.config.mode,
                **self.config.options.get('pandas_kwargs', {})
            )
        else:
            (data.write
             .format("jdbc")
             .option("url", self.config.options['url'])
             .option("dbtable", self.config.table)
             .mode(self.config.mode)
             .save())