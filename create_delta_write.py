from pyspark.sql import SparkSession

import os
from pathlib import Path
repo_root = os.path.dirname(os.path.abspath(__file__))
log4j = os.path.join(repo_root, "log4j.properties")
log4j2 = os.path.join(repo_root, "log4j2.xml")
log4j_uri = Path(log4j).as_uri()
log4j2_uri = Path(log4j2).as_uri()
extra = f"-Dlog4j.configuration={log4j_uri} -Dlog4j2.configurationFile={log4j2_uri}"
spark = (
    SparkSession.builder
    .appName("CreateDeltaWrite")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.extraJavaOptions", extra)
    .config("spark.executor.extraJavaOptions", extra)
    .getOrCreate()
)

table_path = "scratch/example_delta_table"

# Build an empty DataFrame with the desired schema using SQL to avoid Python pickling
empty_df = spark.sql("SELECT CAST(NULL AS INT) AS id, CAST(NULL AS STRING) AS name").limit(0)

# Ensure parent dir exists
import os
os.makedirs(os.path.dirname(table_path), exist_ok=True)

# Write an empty Delta table (this will create the _delta_log and metadata)
empty_df.write.format("delta").mode("overwrite").partitionBy("id").save(table_path)

print('WRITE_COMPLETE')

spark.stop()
