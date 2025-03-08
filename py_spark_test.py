from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("PySparkExample") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Perform a simple transformation
df_filtered = df.filter(df.Age > 30)

# Show the transformed DataFrame
df_filtered.show()

# Stop the SparkSession
spark.stop()
