from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import os
from pathlib import Path
import shutil

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
    # Create the Delta table using a SQL DDL statement to avoid PySpark
    # pickling/serialization when building DataFrames from Python objects.
    # Build column definitions from the schema.simpleString() output.
    try:
        simple = schema.simpleString()  # e.g. "struct<id:int,name:string>"
        inner = simple[simple.find("<") + 1 : -1]
        cols = [c.strip() for c in inner.split(",") if c.strip()]
        type_map = {
            "string": "STRING",
            "int": "INT",
            "integer": "INT",
            "long": "BIGINT",
            "bigint": "BIGINT",
            "double": "DOUBLE",
            "float": "FLOAT",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }
        col_defs = []
        for c in cols:
            if ":" in c:
                name, typ = c.split(":", 1)
                sqltype = type_map.get(typ.lower(), typ.upper())
                col_defs.append(f"{name} {sqltype}")
            else:
                col_defs.append(c)

        col_defs_str = ", ".join(col_defs)

        # If overwrite requested, remove existing path first so CREATE TABLE will write fresh data.
        if overwrite and os.path.exists(table_path):
            if os.path.isdir(table_path):
                shutil.rmtree(table_path)
            else:
                os.remove(table_path)

        # Use a safe table name (derived from the path) and point it to the
        # desired location. Using LOCATION avoids Spark treating the path as a
        # catalog/db.table identifier (which caused 'Database "delta" not found').
        base = os.path.basename(table_path).replace('-', '_').replace('.', '_') or 'delta_table'
        table_name = f"tbl_{base}"
        # Correct ordering: USING <format> comes before PARTITIONED BY and LOCATION
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs_str}) USING delta"
        if partition_by:
            ddl += " PARTITIONED BY (" + ", ".join(partition_by) + ")"
        ddl += f" LOCATION '{table_path}'"

        spark.sql(ddl)
    except Exception:
        # Fallback to writer approach if DDL fails for any reason (last resort).
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(table_path)

if __name__ == "__main__":
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    repo_root = os.path.dirname(os.path.abspath(__file__))
    log4j = os.path.join(repo_root, "log4j.properties")
    # Use file:// URIs so Log4J can load the files on Windows
    log4j2 = os.path.join(repo_root, "log4j2.xml")
    log4j_uri = Path(log4j).as_uri()
    log4j2_uri = Path(log4j2).as_uri()
    extra = f"-Dlog4j.configuration={log4j_uri} -Dlog4j2.configurationFile={log4j2_uri}"
    spark = (
        SparkSession.builder
        .appName("DeltaTableExample")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")  # Try 2.3.0 if 2.4.0 fails
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", extra)
        .config("spark.executor.extraJavaOptions", extra)
        .getOrCreate()
    )
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    table_path = "scratch/example_delta_table"
    create_delta_table(spark, table_path, schema, partition_by=["id"], overwrite=True)

    spark.stop()