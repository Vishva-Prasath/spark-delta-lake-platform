import pytest
from pyspark.sql import SparkSession
from src.quality.rules import DataQualityChecker

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("UnitTests").getOrCreate()

def test_data_quality_checker(spark):
    data = [
        ("ORD_1", "C_1", 100.0, "COMPLETED"),
        ("ORD_2", "C_2", -50.0, "COMPLETED"), # Invalid amount
        (None, "C_3", 200.0, "PENDING")       # Null order_id
    ]
    columns = ["order_id", "customer_id", "total_amount", "order_status"]
    df = spark.createDataFrame(data, columns)

    validator = DataQualityChecker(df)
    valid_df, invalid_df = validator.validate_orders()

    assert valid_df.count() == 1
    assert invalid_df.count() == 2