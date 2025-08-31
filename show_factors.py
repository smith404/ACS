"""
Debug script to show all pattern factor data
"""

from pyspark.sql import SparkSession

def show_all_pattern_factors():
    """Show all pattern factor data to understand the structure."""
    
    # Create Spark session
    spark = SparkSession.builder.appName("ShowAllFactors").getOrCreate()
    
    # Load pattern factors file
    factors_file = "scratch/gold/host_pattern_factor.csv"
    factors_df = spark.read.csv(factors_file, header=True, inferSchema=True)
    
    print("=== All Pattern Factors ===")
    factors_df.orderBy("pattern_id", "period_frequency", "period").show(20, truncate=False)
    
    print("\n=== Summary by Pattern ID ===")
    factors_df.groupBy("pattern_id", "period_frequency").count().orderBy("pattern_id", "period_frequency").show()
    
    spark.stop()

if __name__ == "__main__":
    show_all_pattern_factors()
