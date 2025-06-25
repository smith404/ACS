from delta.tables import DeltaTable
from pyspark.sql import SparkSession

def create_delta_table(spark: SparkSession, table_path: str, schema, partition_by=None, overwrite=False):
    """
    Create a Delta table at the specified path with the given schema.

    Args:
        spark (SparkSession): The Spark session.
        table_path (str): The location to create the Delta table.
        schema (StructType): The schema of the table (pyspark.sql.types.StructType).
        partition_by (list, optional): List of columns to partition by.
        overwrite (bool, optional): Whether to overwrite if table exists.
    """
    mode = "overwrite" if overwrite else "errorifexists"
    df = spark.createDataFrame([], schema)
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(table_path)

if __name__ == "__main__":
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    spark = (
        SparkSession.builder
        .appName("DeltaTableExample")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")  # Try 2.3.0 if 2.4.0 fails
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    table_path = "scratch/example_delta_table"
    create_delta_table(spark, table_path, schema, partition_by=["id"], overwrite=True)

    spark.stop()