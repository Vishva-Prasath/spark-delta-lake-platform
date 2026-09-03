from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataQualityChecker:
    """
    Executes business validation rules and splits data into valid and quarantine DataFrames.
    """
    def __init__(self, df: DataFrame):
        self.df = df

    def validate_orders(self) -> Tuple[DataFrame, DataFrame]:
        """
        Validates order payloads for:
        1. Non-null order_id and customer_id
        2. Positive total_amount
        3. Valid status
        """
        validation_expr = (
            F.col("order_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            (F.col("total_amount") > 0) &
            F.col("order_status").isin("PENDING", "COMPLETED", "CANCELLED", "REFUNDED")
        )

        valid_df = self.df.filter(validation_expr)
        invalid_df = self.df.filter(~validation_expr).withColumn(
            "rejection_reason",
            F.when(F.col("order_id").isNull(), "Null order_id")
             .when(F.col("customer_id").isNull(), "Null customer_id")
             .when(F.col("total_amount") <= 0, "Invalid total_amount")
             .otherwise("Invalid order status")
        ).withColumn("rejected_at", F.current_timestamp())

        return valid_df, invalid_df