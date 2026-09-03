import os
import sys
from src.utils.spark_session import get_spark_session
from src.ingestion.bronze_loader import BronzeIngestor
from src.transformations.silver_merger import SilverMerger
from src.transformations.gold_aggregator import GoldAggregator
from src.maintenance.delta_optimizer import optimize_and_cluster

def main():
    base_dir = os.getcwd() # current working directory base_dir  = C:/projects/spark-delta-lake-platform
    raw_data_path = f"{base_dir}/sample_data/orders.json"
    bronze_path = f"{base_dir}/storage/bronze/orders"
    silver_path = f"{base_dir}/storage/silver/orders"
    quarantine_path = f"{base_dir}/storage/quarantine/orders"
    gold_path = f"{base_dir}/storage/gold/daily_metrics"

    spark = get_spark_session("ProductionPipeline")

    try:
        # Step 1: Bronze Ingestion 
        ingestor = BronzeIngestor(spark)
        ingestor.ingest_raw_json(raw_data_path, bronze_path)

        # Step 2: Silver Merge with Data Quality
        merger = SilverMerger(spark)
        merger.process_and_merge(bronze_path, silver_path, quarantine_path)

        # Step 3: Gold Aggregation
        aggregator = GoldAggregator(spark)
        aggregator.build_daily_metrics(silver_path, gold_path)

        # Step 4: Table Maintenance
        optimize_and_cluster(spark, silver_path, zorder_col="customer_id")

    except Exception as e:
        print(f"Pipeline Failure: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()