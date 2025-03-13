from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, avg

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("PySparkExample") \
    .getOrCreate()

uv_data = [("B1700", 34), ("A1100", 34), ("C1100", 45), ("A1100", 29), ("C1100", 59)]
uv_columns = ["TOA", "UV"]
uv_df = spark.createDataFrame(uv_data, uv_columns)

uv_df.show()

toa_mappings = {
    'B1700': 3410,
    'C1100': 2400,
    'A1100': 1400
}

# Apply toa_mappings to update uv_df with updated TOA values
uv_df = uv_df.withColumn("TOA", col("TOA").cast("string"))
for key, value in toa_mappings.items():
    uv_df = uv_df.withColumn("TOA", when(col("TOA") == key, lit(value)).otherwise(col("TOA")))

uv_df.show()

# Create a simple DataFrame
data = [("John", 37, "M"), ("Alice", 64, "F"), ("Bob", 45, "M"), ("Cathy", 29, "F"), ("Cathy", 59, "F")]
columns = ["Name", "Age", "Gender"]
df = spark.createDataFrame(data, columns)

# Rename the column "Age" to "CurrentAge"
df = df.withColumnRenamed("Age", "CurrentAge")

# Add a new column "CityOfBirth" with the value "Middlesbrough"
df = df.withColumn("CityOfBirth", lit("Middlesbrough"))

# Replace the name "Cathy" with "Catherine" if the age is older than 40
df = df.withColumn("Name", when((df.Name == "Cathy") & (df.CurrentAge > 40), "Catherine").otherwise(df.Name))

# Add a new column "MonthsOld" that is the age multiplied by 12
df = df.withColumn("MonthsOld", col("CurrentAge") * 12)

# Show the DataFrame
df.show()

# Group by "Gender" and calculate the average "MonthsOld"
df_grouped = df.groupBy("Gender").agg(avg("MonthsOld").alias("AverageMonthsOld"))

# Show the grouped DataFrame
df_grouped.show()

# Perform a simple transformation
df_filtered = df.filter(df.CurrentAge > 30)

# Set the Name in df_filtered to be "<redacted>"
df_filtered = df_filtered.withColumn("Name", lit("<redacted>"))

# Show the transformed DataFrame
df_filtered.show()

# Sort df_filtered by MonthsOld from oldest to youngest
df_filtered_sorted = df_filtered.orderBy(col("MonthsOld").desc())

# Show the sorted DataFrame
df_filtered_sorted.show()

# Stop the SparkSession
spark.stop()
