import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def test_delta_lake_capabilities():
    # 1. Initialize Spark Session with Delta extensions
    spark = SparkSession.builder \
        .appName("DeltaFeatureTesting") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    silver_path = os.path.join(os.getcwd(), "storage", "silver", "orders")

    if not DeltaTable.isDeltaTable(spark, silver_path):
        print(f"Delta table not found at {silver_path}. Run 'python -m jobs.run_pipeline' first.")
        return

    # Instantiated DeltaTable object
    delta_table = DeltaTable.forPath(spark, silver_path)

    print("\n" + "="*50)
    print("1. FEATURE: DELTA TRANSACTION HISTORY & TIME TRAVEL")
    print("="*50)
    
    # View commit history (_delta_log)
    history_df = delta_table.history()
    history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

    # Time Travel: Query Version 0
    print("--- Reading Silver Table at VERSION AS OF 0 ---")
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
    df_v0.show()

    print("\n" + "="*50)
    print("2. FEATURE: SCHEMA ENFORCEMENT & EVOLUTION")
    print("="*50)
    
    # Attempt to write incompatible schema (Schema Enforcement)
    bad_data = [("ORD_999", "INVALID_SCHEMA_DATA")]
    bad_df = spark.createDataFrame(bad_data, ["order_id", "unexpected_column"])
    
    try:
        bad_df.write.format("delta").mode("append").save(silver_path)
    except Exception as e:
        print("✅ Schema Enforcement Triggered Successfully! Prevented corrupt write.")

    print("\n" + "="*50)
    print("3. FEATURE: COMPACTION & Z-ORDERING (DATA SKIPPING)")
    print("="*50)
    
    # Run Z-Order optimization on frequently filtered join keys
    print("Running OPTIMIZE with Z-ORDER BY (customer_id)...")
    delta_table.optimize().executeZOrderBy("customer_id")
    print("✅ Optimization Complete!")

    print("\n" + "="*50)
    print("4. FEATURE: ACID MERGE (SCD TYPE 1 UPSERT)")
    print("="*50)
    
    # Supply full schema matching target Silver table
    updates_df = spark.createDataFrame([
        ("CUST_100", "ORD_501", "COMPLETED", 999.99, "2026-09-04T12:00:00Z", "2026-09-04 12:00:00", "rest_api_payload.json")
    ], ["customer_id", "order_id", "order_status", "total_amount", "updated_at", "_ingested_at", "_source_file"]) \
    .withColumn("_processed_at", F.current_timestamp())

    # Fix: Call alias() on delta_table instance, not DeltaTable class
    (
        delta_table.alias("target")
        .merge(updates_df.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdate(set={
            "customer_id": "source.customer_id",
            "total_amount": "source.total_amount",
            "order_status": "source.order_status",
            "updated_at": "source.updated_at",
            "_processed_at": "source._processed_at"
        })
        .whenNotMatchedInsert(values={
            "order_id": "source.order_id",
            "customer_id": "source.customer_id",
            "total_amount": "source.total_amount",
            "order_status": "source.order_status",
            "updated_at": "source.updated_at",
            "_ingested_at": "source._ingested_at",
            "_source_file": "source._source_file",
            "_processed_at": "source._processed_at"
        })
        .execute()
    )
    print("✅ Merge Executed! Updated ORD_501 total_amount to 999.99.")

    print("\n" + "="*50)
    print("5. FEATURE: STORAGE MAINTENANCE (VACUUM)")
    print("="*50)
    
    # Retain 168 hours (default 7 days) or disable safety check for testing
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
    print("Inspecting table files before vacuum...")
    
    # Fix: Call vacuum() on delta_table instance, not DeltaTable class
    delta_table.vacuum(72)  
    print("✅ Vacuum completed successfully.")

if __name__ == "__main__":
    test_delta_lake_capabilities()