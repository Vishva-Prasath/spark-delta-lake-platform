import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "DeltaLakeProductionPlatform") -> SparkSession:
    """
    Constructs an enterprise-tuned SparkSession with Delta Lake support,
    AQE enabled, and optimized shuffle partitions.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # AQE Optimizations
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Performance & Memory Tuning
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return builder.getOrCreate()