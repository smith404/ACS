from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, avg

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("PySparkExample") \
    .getOrCreate()

# Create a simple DataFrame
data = [("John", 34, "M"), ("Alice", 34, "F"), ("Bob", 45, "M"), ("Cathy", 29, "F"), ("Cathy", 59, "F")]
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
#df.groupBy("year", "sex").agg(avg("percent"), count("*"))

# Show the grouped DataFrame
df_grouped.show()

# Perform a simple transformation
df_filtered = df.filter(df.CurrentAge > 30)

# Show the transformed DataFrame
df_filtered.show()

# Stop the SparkSession
spark.stop()
