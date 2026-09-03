from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.utils.logger import StructuredLogger

logger = StructuredLogger("BronzeIngestor")

class BronzeIngestor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_raw_json(self, input_path: str, bronze_path: str):
        """
        Appends raw records into the Bronze layer preserving schema on read,
        adding ingestion metadata.
        """
        logger.info("Starting Bronze Ingestion", {"input_path": input_path})
        
        raw_df = self.spark.read.option("multiline", "true").json(input_path)
        
        bronze_df = (
            raw_df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )
        
        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )
        
        logger.info("Successfully ingested records into Bronze", {"count": bronze_df.count()})