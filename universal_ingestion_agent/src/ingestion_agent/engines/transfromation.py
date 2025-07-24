import polars as pl
import pandas as pd
from pyspark.sql import SparkSession
import psutil
import logging
from typing import Union, List, Dict, Optional


class TransformationEngine:
    def __init__(self):
        self.spark = None
        self._spark_initialized = False
        self._setup_logging()
        self._init_thresholds()

    def _setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _init_thresholds(self):
        """Initialize resource thresholds"""
        self.MEMORY_THRESHOLD_MB = 1024  # Switch to Spark beyond this
        self.CPU_THRESHOLD = 0.7  # 70% CPU usage
        self.MIN_SPARK_PARTITIONS = 4
        self.PARTITION_SIZE_MB = 128  # Target partition size

    def _init_spark(self):
        """Lazy initialization of Spark session"""
        if not self._spark_initialized:
            try:
                self.spark = SparkSession.builder \
                    .appName("AutoScaleTransformer") \
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                    .config("spark.executor.memory", self._calc_spark_memory()) \
                    .getOrCreate()
                self._spark_initialized = True
                self.logger.info("Spark session initialized")
            except Exception as e:
                self.logger.error(f"Spark initialization failed: {str(e)}")
                raise

    def _calc_spark_memory(self) -> str:
        """Calculate optimal Spark memory allocation"""
        total_mem_gb = psutil.virtual_memory().total // (1024 ** 3)
        executor_mem = max(4, total_mem_gb - 2)  # Leave 2GB for system
        return f"{executor_mem}g"

    def _estimate_data_size(self, data: Union[List[Dict], pl.DataFrame, pd.DataFrame]) -> float:
        """Estimate data size in MB with sampling for lists"""
        if isinstance(data, list):
            if not data:
                return 0.0
            # Sample first 1000 items or 1% (whichever is smaller)
            sample_size = min(1000, max(1, len(data) // 100))
            sample = data[:sample_size]
            df_sample = pl.DataFrame(sample)
            sample_size_mb = df_sample.estimated_size("mb")
            return sample_size_mb * (len(data) / sample_size)
        elif isinstance(data, pl.DataFrame):
            return data.estimated_size("mb")
        elif isinstance(data, pd.DataFrame):
            return data.memory_usage(deep=True).sum() / (1024 ** 2)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    def _should_use_spark(self, estimated_size_mb: float) -> bool:
        """Determine execution engine based on resources"""
        if estimated_size_mb > self.MEMORY_THRESHOLD_MB:
            return True

        mem_available = psutil.virtual_memory().available / (1024 ** 2)
        cpu_usage = psutil.cpu_percent() / 100

        return (estimated_size_mb > mem_available * 0.3) or (cpu_usage > self.CPU_THRESHOLD)

    def _prepare_dataframe(self, data: Union[List[Dict], pl.DataFrame, pd.DataFrame]) -> pl.DataFrame:
        """Convert input to Polars DataFrame"""
        if isinstance(data, list):
            return pl.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            return pl.from_pandas(data)
        elif isinstance(data, pl.DataFrame):
            return data
        else:
            raise ValueError(f"Unsupported input type: {type(data)}")

    def transform(
            self,
            data: Union[List[Dict], pl.DataFrame, pd.DataFrame],
            transformations: List[Dict],
            force_engine: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Transform data using automatic engine selection.

        Args:
            data: Input data (list of dicts, Polars DF, or Pandas DF)
            transformations: List of transformation steps
            force_engine: Optional override ('spark' or 'polars')

        Returns:
            Polars DataFrame with transformations applied
        """
        try:
            # Convert input to Polars and estimate size
            df = self._prepare_dataframe(data)
            estimated_size = self._estimate_data_size(data)
            self.logger.info(f"Data size estimate: {estimated_size:.2f}MB")

            # Engine selection
            use_spark = (force_engine == 'spark') or (
                    (force_engine != 'polars') and
                    self._should_use_spark(estimated_size)
            )

            if use_spark:
                self.logger.info("Using Spark engine")
                return self._spark_transform(df, transformations)
            else:
                self.logger.info("Using native Polars")
                return self._polars_transform(df, transformations)

        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise

    def _polars_transform(self, df: pl.DataFrame, transformations: List[Dict]) -> pl.DataFrame:
        """Execute transformations using native Polars"""
        lf = df.lazy()  # Use lazy API for optimization

        for transform in transformations:
            try:
                if transform["type"] == "filter":
                    lf = lf.filter(eval(transform["condition"]))
                elif transform["type"] == "aggregate":
                    lf = lf.group_by(transform["by"]).agg(
                        [eval(expr) for expr in transform["exprs"]]
                    )
                elif transform["type"] == "select":
                    lf = lf.select([eval(expr) for expr in transform["columns"]])
            except Exception as e:
                self.logger.error(f"Failed to apply {transform['type']}: {str(e)}")
                raise

        return lf.collect()

    def _spark_transform(self, df: pl.DataFrame, transformations: List[Dict]) -> pl.DataFrame:
        """Execute transformations using Spark"""
        self._init_spark()

        # Convert to Spark DataFrame
        spark_df = self.spark.createDataFrame(df.to_pandas())

        # Apply transformations
        for transform in transformations:
            try:
                if transform["type"] == "filter":
                    spark_df = spark_df.filter(transform["condition"])
                elif transform["type"] == "aggregate":
                    spark_df = spark_df.groupBy(transform["by"]).agg(*transform["exprs"])
                elif transform["type"] == "select":
                    spark_df = spark_df.select(transform["columns"])
            except Exception as e:
                self.logger.error(f"Spark {transform['type']} failed: {str(e)}")
                raise

        # Convert back to Polars efficiently
        return pl.from_pandas(spark_df.toPandas())

    def __del__(self):
        """Cleanup resources"""
        if self._spark_initialized and self.spark:
            try:
                self.spark.stop()
                self.logger.info("Spark session stopped")
            except Exception as e:
                self.logger.error(f"Spark shutdown error: {str(e)}")