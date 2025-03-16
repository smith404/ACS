from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, avg, split

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("PySparkExample") \
    .getOrCreate()

uv_data = [("B1700", 34, "Q1_2024"), ("A1100", 34, "Q2_2024"), ("C1100", 45, "Q3_2024"), ("A1100", 29, "Q4_2024"), ("C1100", 59, "Q4_2024")]
uv_columns = ["TOA", "UV", "FIN_PERIOD"]
uv_df = spark.createDataFrame(uv_data, uv_columns)

uv_df.show()

toa_mappings = {
    'B1700': "3410",
    'C1100': "2400",
    'A1100': "1400"
}

# Apply toa_mappings to update uv_df with updated TOA values
uv_df = uv_df.withColumn("TOA", col("TOA").cast("string"))
for key, value in toa_mappings.items():
    uv_df = uv_df.withColumn("TOA", when(col("TOA") == lit(key), lit(value)).otherwise(col("TOA")))

# Add columns quarter and year by splitting column FIN_PERIOD using the '_' character
uv_df = uv_df.withColumn("quarter", split(col("FIN_PERIOD"), "_").getItem(0))
uv_df = uv_df.withColumn("year", split(col("FIN_PERIOD"), "_").getItem(1))

uv_df.show()

# Create a simple DataFrame
data = [("John", 37, "M", "3410", "Q1_2024"), ("Alice", 64, "F", "1400", "Q2_2024"), ("Bob", 45, "M", "1400", "Q3_2024"), ("Cathy", 29, "F", "3410", "Q4_2024"), ("Cathy", 59, "F", "2400", "Q4_2024")]
columns = ["Name", "Age", "Gender", "TOA", "FIN_PERIOD"]
df = spark.createDataFrame(data, columns)



# Rename the column "Age" to "CurrentAge"
df = df.withColumnRenamed("Age", "CurrentAge")

# Add a new column "CityOfBirth" with the value "Zürich"
df = df.withColumn("CityOfBirth", lit("Zürich"))

# Replace the name "Cathy" with "Catherine" if the age is older than 40
df = df.withColumn("Name", when((df.Name == "Cathy") & (df.CurrentAge > 40), "Catherine").otherwise(df.Name))

# Add a new column "MonthsOld" that is the age multiplied by 12
df = df.withColumn("MonthsOld", col("CurrentAge") * 12)

# Show the DataFrame
df.show()

# Create a new DataFrame based on df with columns: "Age", "Gender", "FIN_PERIOD"
df_new = df.select("CurrentAge", "Gender", "FIN_PERIOD").withColumnRenamed("CurrentAge", "Age")

# Show the new DataFrame
df_new.show()

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

# Join df_filtered_sorted with uv_df using columns TOA and FIN_PERIOD
df_joined = df_filtered_sorted.join(uv_df, on=["TOA", "FIN_PERIOD"], how="inner")

# Show the joined DataFrame
df_joined.show()

# Make all column names in df_joined lowercase
df_joined = df_joined.toDF(*[c.lower() for c in df_joined.columns])

# Remove the column TOA from df_joined
df_joined = df_joined.drop("toa")

df_joined.show()

# Stop the SparkSession
spark.stop()
