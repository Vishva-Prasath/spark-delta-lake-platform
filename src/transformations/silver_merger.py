from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.quality.rules import DataQualityChecker
from src.utils.logger import StructuredLogger

logger = StructuredLogger("SilverMerger")

class SilverMerger:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def process_and_merge(self, bronze_path: str, silver_path: str, quarantine_path: str):
        """
        Reads Bronze data, validates via DQ framework, quarantines bad records,
        and performs an idempotent MERGE (UPSERT) into Silver.
        """
        logger.info("Starting Silver Upsert Process")
        
        bronze_df = self.spark.read.format("delta").load(bronze_path)
        
        # Apply Data Quality Gate
        validator = DataQualityChecker(bronze_df)
        valid_df, invalid_df = validator.validate_orders()

        # Handle Quarantine
        if invalid_df.count() > 0:
            logger.error("Data Quality Failures Found", {"count": invalid_df.count()})
            invalid_df.write.format("delta").mode("append").save(quarantine_path)

        # Deduplicate incoming valid batch based on event timestamp
        window_spec = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
        deduped_df = valid_df.withColumn("row_num", F.row_number().over(window_spec)) \
                             .filter("row_num = 1") \
                             .drop("row_num")

        # Create Silver Delta Table if it doesn't exist
        if not DeltaTable.isDeltaTable(self.spark, silver_path):
            deduped_df.write.format("delta").mode("overwrite").save(silver_path)
            logger.info("Initialized Silver Delta Table")
            return

        # Perform MERGE (SCD Type 1)
        silver_table = DeltaTable.forPath(self.spark, silver_path)
        
        (
            silver_table.alias("target")
            .merge(
                deduped_df.alias("source"),
                "target.order_id = source.order_id"
            )
            .whenMatchedUpdate(set={
                "customer_id": "source.customer_id",
                "total_amount": "source.total_amount",
                "order_status": "source.order_status",
                "updated_at": "source.updated_at",
                "_processed_at": "F.current_timestamp()"
            })
            .whenNotMatchedInsert(values={
                "order_id": "source.order_id",
                "customer_id": "source.customer_id",
                "total_amount": "source.total_amount",
                "order_status": "source.order_status",
                "updated_at": "source.updated_at",
                "_processed_at": "F.current_timestamp()"
            })
            .execute()
        )
        logger.info("Silver Layer Merge Executed Successfully")