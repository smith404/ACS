"""
Test script for CoveragePatternReader
"""

from pyspark.sql import SparkSession
from coverage_pattern_reader import create_coverage_pattern_reader
import os


def test_coverage_pattern_reader():
    """Test the CoveragePatternReader with sample data."""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("TestCoveragePatternReader") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        # Initialize the reader
        reader = create_coverage_pattern_reader(spark, "scratch/gold")
        
        print("=== Testing Coverage Pattern Reader ===\n")
        
        # Test 1: Get pattern for coverage_id 100
        print("Test 1: Coverage ID 100")
        pattern_obj = reader.construct_pattern_object(100)
        if pattern_obj:
            print(f"  {pattern_obj}")
            print(f"  Upfront factors: {len(pattern_obj.upfront_factors)}")
            print(f"  Distributed factors: {len(pattern_obj.distributed_factors)}")
        else:
            print("  No pattern found")
        print()
        
        # Test 2: Get pattern for coverage_id 101
        print("Test 2: Coverage ID 101")
        pattern_obj = reader.construct_pattern_object(101)
        if pattern_obj:
            print(f"  {pattern_obj}")
            print(f"  Upfront factors: {pattern_obj.upfront_factors}")
            print(f"  Distributed factors: {pattern_obj.distributed_factors}")
        else:
            print("  No pattern found")
        print()
        
        # Test 3: Get summary for coverage_id 101
        print("Test 3: Pattern Summary for Coverage ID 101")
        summary = reader.get_pattern_summary(101)
        for key, value in summary.items():
            print(f"  {key}: {value}")
        print()
        
        # Test 4: Batch process multiple coverage IDs
        print("Test 4: Batch Processing")
        coverage_ids = [100, 101, 999]  # Include a non-existent ID
        batch_results = reader.batch_process_coverages(coverage_ids)
        for result in batch_results:
            cov_id = result.get('coverage_id', 'Unknown')
            if 'error' in result:
                print(f"  Coverage {cov_id}: {result['error']}")
            else:
                print(f"  Coverage {cov_id}: Pattern {result['pattern_id']} "
                      f"(Upfront: {result['total_upfront_factor']:.4f}, "
                      f"Distributed: {result['total_distributed_factor']:.4f})")
        
        print("\n=== Test completed successfully ===")
        
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    test_coverage_pattern_reader()
