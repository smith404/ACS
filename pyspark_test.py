from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("PySparkExample") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29), ("Cathy", 59)]
columns = ["Name", "Age"]
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

# Perform a simple transformation
df_filtered = df.filter(df.CurrentAge > 30)

# Show the transformed DataFrame
df_filtered.show()

# Stop the SparkSession
spark.stop()
