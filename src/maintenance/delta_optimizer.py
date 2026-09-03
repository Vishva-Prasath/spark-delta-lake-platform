from delta.tables import DeltaTable
from pyspark.sql import SparkSession

def optimize_and_cluster(spark: SparkSession, table_path: str, zorder_col: str):
    """
    Solves small file problem via OPTIMIZE and optimizes data layout using Z-Ordering.
    """
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Run OPTIMIZE with Z-Order
    delta_table.optimize().executeZOrderBy(zorder_col)
    
    # Clean up older uncommitted files (Vacuum)
    # Default retention is 168 hours (7 days)
    delta_table.vacuum(72)