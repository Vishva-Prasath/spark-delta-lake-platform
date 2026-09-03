from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.utils.logger import StructuredLogger

logger = StructuredLogger("GoldAggregator")

class GoldAggregator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_daily_metrics(self, silver_path: str, gold_path: str):   
        """
        Aggregates Silver tier orders data into daily business KPIs.
        """
        logger.info("Building Gold Layer Metrics")
        
        silver_df = self.spark.read.format("delta").load(silver_path)
        
        gold_df = (
            silver_df
            .withColumn("order_date", F.to_date("updated_at"))
            .groupBy("order_date", "order_status")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("gross_revenue"),
                F.avg("total_amount").alias("avg_order_value")
            )
        )
        
        gold_df.write.format("delta").mode("overwrite").save(gold_path)
        logger.info("Gold Metrics Written Successfully")