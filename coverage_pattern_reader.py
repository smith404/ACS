"""
Coverage Pattern Allocation Reader for Databricks
Reads coverage allocation data and constructs pattern objects with QS (upfront) and Q (distributed) portions.
Uses existing Pattern and PatternFactor classes for consistency.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from typing import Dict, List, Optional, Tuple
import os
from datetime import date

# Import existing pattern classes
from pattern import Pattern
from pattern_factor import PatternFactor, Type
from calendar_factory import CalendarFactory, TimeUnit
from exposure_matrix import ExposureMatrix


class CoveragePatternReader:
    """Reads coverage pattern allocation data using PySpark for Databricks."""
    
    def __init__(self, spark: SparkSession, data_path: str = "scratch/gold"):
        self.spark = spark
        self.data_path = data_path
        self._coverage_allocation_df = None
        self._pattern_factors_df = None
        self._pattern_df = None
    
    def _load_coverage_allocation(self):
        """Load coverage allocation data."""
        if self._coverage_allocation_df is None:
            file_path = f"{self.data_path}/host_coverage_pattern_allocation.csv"
            self._coverage_allocation_df = self.spark.read.csv(
                file_path, 
                header=True, 
                inferSchema=True
            )
        return self._coverage_allocation_df
    
    def _load_pattern_factors(self):
        """Load pattern factors data."""
        if self._pattern_factors_df is None:
            file_path = f"{self.data_path}/host_pattern_factor.csv"
            self._pattern_factors_df = self.spark.read.csv(
                file_path, 
                header=True, 
                inferSchema=True
            )
        return self._pattern_factors_df
    
    def _load_patterns(self):
        """Load pattern metadata."""
        if self._pattern_df is None:
            file_path = f"{self.data_path}/host_pattern.csv"
            self._pattern_df = self.spark.read.csv(
                file_path, 
                header=True, 
                inferSchema=True
            )
        return self._pattern_df
    
    def get_writing_premium_pattern_id(self, coverage_id: int) -> Optional[Tuple[int, date]]:
        """Get the writing premium pattern ID and start date for a given coverage ID."""
        coverage_df = self._load_coverage_allocation()
        
        # Filter for the specific coverage_id
        result = coverage_df.filter(col("coverage_id") == coverage_id).collect()
        
        if not result:
            return None
        
        # Access the columns using dot notation
        pattern_id = result[0].writing_premium_pattern_id
        start_date_timestamp = result[0].pattern_start_date
        
        # Convert timestamp to date object
        if hasattr(start_date_timestamp, 'date'):
            # If it's a datetime object, extract the date
            start_date = start_date_timestamp.date()
        else:
            # If it's a string, parse it
            start_date = date.fromisoformat(str(start_date_timestamp).split()[0])
        
        return pattern_id, start_date
    
    def construct_pattern_object(self, coverage_id: int) -> Optional[Pattern]:
        """
        Construct a pattern object for a given coverage ID using existing Pattern class.
        
        Args:
            coverage_id: The coverage ID to look up
            
        Returns:
            Pattern object with QS (upfront) and Q (distributed) portions, or None if not found
        """
        # Step 1: Get the writing premium pattern ID and start date
        pattern_data = self.get_writing_premium_pattern_id(coverage_id)
        if pattern_data is None:
            print(f"No pattern found for coverage_id: {coverage_id}")
            return None
        
        pattern_id, start_date = pattern_data
        
        # Step 2: Get pattern metadata
        pattern_df = self._load_patterns()
        pattern_info = pattern_df.filter(col("pattern_id") == pattern_id).collect()
        pattern_name = pattern_info[0].pattern_name if pattern_info else f"Pattern_{pattern_id}"
        
        # Step 3: Create pattern object using existing Pattern class
        pattern_obj = Pattern()
        # Add custom attributes for pattern metadata
        pattern_obj.name = pattern_name
        pattern_obj.pattern_id = pattern_id
        pattern_obj.start_date = start_date
        
        # Step 4: Get pattern factors
        factors_df = self._load_pattern_factors()
        pattern_factors = factors_df.filter(col("pattern_id") == pattern_id).collect()
        
        if not pattern_factors:
            print(f"No factors found for pattern_id: {pattern_id}")
            return pattern_obj
        
        # Step 5: Group factors by period and combine QS/Q into single PatternFactor objects
        # First, organize factors by period
        factors_by_period = {}
        for factor_row in pattern_factors:
            period_frequency = factor_row.period_frequency
            period = factor_row.period
            factor_value = factor_row.factor_value
            duration = int(factor_row.duration)  # Convert to integer for date calculations
            
            # Validate duration - log error if less than 1
            if duration < 1:
                print(f"ERROR: Pattern factor with duration < 1 found in writing_premium_pattern_id {pattern_id}")
                print(f"       Period: {period}, Frequency: {period_frequency}, Duration: {duration}, Value: {factor_value}")
                print(f"       Coverage ID: {coverage_id}")
            
            if period not in factors_by_period:
                factors_by_period[period] = {}
            
            factors_by_period[period][period_frequency] = {
                'value': factor_value,
                'duration': duration
            }
        
        # Now create PatternFactor objects, combining QS and Q for same periods
        for period, period_data in factors_by_period.items():
            # Extract QS (upfront) and Q (distributed) data
            qs_data = period_data.get('QS', {'value': 0.0, 'duration': 1})
            q_data = period_data.get('Q', {'value': 0.0, 'duration': 1})
            
            # Create combined PatternFactor
            pattern_factor = PatternFactor(
                type_=Type.QUARTER,
                up_front=qs_data['value'],              # QS value for upfront
                distribution=q_data['value'],           # Q value for distribution
                up_front_duration=qs_data['duration'],  # QS duration for upfront
                distribution_duration=q_data['duration'] # Q duration for distribution
            )
            
            # Add custom attributes to match CSV data
            pattern_factor.period = period
            pattern_factor.duration = max(qs_data['duration'], q_data['duration'])  # Use max duration
            pattern_factor.factor_type = Type.QUARTER  # For compatibility
            
            pattern_obj.add_element(pattern_factor)
        
        return pattern_obj
    
    def generate_pattern_factors(self, coverage_id: int) -> Optional[List]:
        """
        Generate pattern factors for a given coverage ID using the pattern start date.
        
        Args:
            coverage_id: The coverage ID to generate factors for
            
        Returns:
            List of Factor objects generated from the pattern, or None if pattern not found
        """
        pattern = self.construct_pattern_object(coverage_id)
        if pattern is None:
            return None
        
        # Generate factors using the pattern's start date
        factors = pattern.get_factors(pattern.start_date, use_calendar=True)
        return factors
    
    def calculate_exposure_matrix(self, coverage_id: int) -> Optional[ExposureMatrix]:
        """
        Calculate exposure matrix for a given coverage ID using financial quarters.
        
        Args:
            coverage_id: The coverage ID to calculate exposure matrix for
            
        Returns:
            ExposureMatrix object with financial quarter buckets, or None if pattern not found
        """
        factors = self.generate_pattern_factors(coverage_id)
        if factors is None:
            return None
        
        # Initialize calendar factory
        calendar_factory = CalendarFactory()
        
        # Get financial quarter end dates
        incurred_financial_quarter_dates = calendar_factory.get_financial_quarter_end_dates(factors, use_incurred=True)
        exposed_financial_quarter_dates = calendar_factory.get_financial_quarter_end_dates(factors, use_incurred=False)
        
        # Create exposure matrix
        matrix = ExposureMatrix(
            factors=factors,
            incurred_bucket_dates=incurred_financial_quarter_dates,
            exposure_bucket_dates=exposed_financial_quarter_dates,
            to_end=True
        )
        
        return matrix
    
    def get_exposure_vector(self, coverage_id: int) -> Optional[List]:
        """
        Get exposure vector for a given coverage ID.
        
        Args:
            coverage_id: The coverage ID to get exposure vector for
            
        Returns:
            List of ExposureVectorEntry objects, or None if pattern not found
        """
        matrix = self.calculate_exposure_matrix(coverage_id)
        if matrix is None:
            return None
        
        return matrix.get_exposure_vector()
    
    def get_incurred_vector(self, coverage_id: int) -> Optional[List]:
        """
        Get incurred vector for a given coverage ID.
        
        Args:
            coverage_id: The coverage ID to get incurred vector for
            
        Returns:
            List of ExposureVectorEntry objects, or None if pattern not found
        """
        matrix = self.calculate_exposure_matrix(coverage_id)
        if matrix is None:
            return None
        
        return matrix.get_incurred_vector()
    
    def get_pattern_analysis(self, coverage_id: int) -> Dict:
        """
        Get comprehensive pattern analysis including factors, matrix, and vectors.
        
        Args:
            coverage_id: The coverage ID to analyze
            
        Returns:
            Dictionary with pattern analysis data
        """
        pattern = self.construct_pattern_object(coverage_id)
        if pattern is None:
            return {"error": f"No pattern found for coverage_id: {coverage_id}"}
        
        factors = self.generate_pattern_factors(coverage_id)
        matrix = self.calculate_exposure_matrix(coverage_id)
        
        if factors is None or matrix is None:
            return {"error": f"Failed to generate factors or matrix for coverage_id: {coverage_id}"}
        
        exposure_vector = matrix.get_exposure_vector()
        incurred_vector = matrix.get_incurred_vector()
        
        # Calculate date ranges
        calendar_factory = CalendarFactory()
        min_incurred, max_incurred = calendar_factory.get_factor_date_range(factors, use_incurred=True)
        min_exposed, max_exposed = calendar_factory.get_factor_date_range(factors, use_incurred=False)
        
        return {
            "coverage_id": coverage_id,
            "pattern_id": getattr(pattern, 'pattern_id', None),
            "pattern_name": pattern.name,
            "pattern_start_date": getattr(pattern, 'start_date', None).isoformat() if hasattr(pattern, 'start_date') else None,
            "total_elements": len(pattern.elements),
            "factors_count": len(factors),
            "matrix_entries_count": len(matrix.get_matrix_entries()),
            "matrix_total_sum": matrix.get_total_sum(),
            "exposure_vector_count": len(exposure_vector),
            "incurred_vector_count": len(incurred_vector),
            "date_ranges": {
                "min_incurred": min_incurred.isoformat() if min_incurred else None,
                "max_incurred": max_incurred.isoformat() if max_incurred else None,
                "min_exposed": min_exposed.isoformat() if min_exposed else None,
                "max_exposed": max_exposed.isoformat() if max_exposed else None
            },
            "exposure_vector": [
                {"date_bucket": entry.date_bucket.isoformat(), "sum": entry.sum}
                for entry in exposure_vector
            ],
            "incurred_vector": [
                {"date_bucket": entry.date_bucket.isoformat(), "sum": entry.sum}
                for entry in incurred_vector
            ]
        }
    
    def create_loss_incurred_pattern(self, coverage_id: int) -> Optional[Pattern]:
        """
        Create a loss incurred pattern from the exposure vector of a writing pattern.
        
        Args:
            coverage_id: The coverage ID to create loss incurred pattern for
            
        Returns:
            Pattern object representing the loss incurred pattern, or None if not found
        """
        # Get the original writing pattern info
        pattern_data = self.get_writing_premium_pattern_id(coverage_id)
        if pattern_data is None:
            print(f"No writing pattern found for coverage_id: {coverage_id}")
            return None
        
        original_pattern_id, _ = pattern_data
        
        # Get exposure vector from the writing pattern
        exposure_vector = self.get_exposure_vector(coverage_id)
        if exposure_vector is None or len(exposure_vector) == 0:
            print(f"No exposure vector found for coverage_id: {coverage_id}")
            return None
        
        # Create new loss incurred pattern
        loss_pattern = Pattern()
        loss_pattern.name = f"Loss incurred pattern for writing pattern {original_pattern_id}"
        loss_pattern.pattern_id = f"LI_{original_pattern_id}"  # LI = Loss Incurred
        
        # Pattern start date is the date of the first exposure vector entry
        loss_pattern.start_date = exposure_vector[0].date_bucket
        
        # Create QE pattern factors from exposure vector entries
        for exposure_entry in exposure_vector:
            # Create PatternFactor with QE type (Quarter Exposure)
            pattern_factor = PatternFactor(
                type_=Type.QUARTER,
                up_front=0.0,                    # No upfront for QE factors
                distribution=exposure_entry.sum, # Exposure value as distribution
                up_front_duration=1,             # Default duration
                distribution_duration=0          # QE factors have duration 0
            )
            
            # Add custom attributes for QE factors
            pattern_factor.period_frequency = "QE"  # Quarter Exposure
            pattern_factor.exposure_date = exposure_entry.date_bucket
            pattern_factor.duration = 0  # QE factors have duration 0
            pattern_factor.factor_type = Type.QUARTER
            
            loss_pattern.add_element(pattern_factor)
        
        return loss_pattern
    
    def get_loss_incurred_pattern_summary(self, coverage_id: int) -> Dict:
        """
        Get summary of the loss incurred pattern created from exposure vector.
        
        Args:
            coverage_id: The coverage ID to get loss incurred pattern summary for
            
        Returns:
            Dictionary with loss incurred pattern summary
        """
        loss_pattern = self.create_loss_incurred_pattern(coverage_id)
        if loss_pattern is None:
            return {"error": f"Failed to create loss incurred pattern for coverage_id: {coverage_id}"}
        
        # Get QE factors details
        qe_factors = [
            {
                'exposure_date': getattr(element, 'exposure_date', None).isoformat() if hasattr(element, 'exposure_date') else None,
                'factor_value': element.distribution,
                'duration': getattr(element, 'duration', 0),
                'period_frequency': getattr(element, 'period_frequency', 'QE')
            }
            for element in loss_pattern.elements
        ]
        
        total_distribution = sum(element.distribution for element in loss_pattern.elements)
        
        return {
            "coverage_id": coverage_id,
            "original_writing_pattern_id": self.get_writing_premium_pattern_id(coverage_id)[0] if self.get_writing_premium_pattern_id(coverage_id) else None,
            "loss_pattern_id": getattr(loss_pattern, 'pattern_id', None),
            "loss_pattern_name": loss_pattern.name,
            "loss_pattern_start_date": loss_pattern.start_date.isoformat() if hasattr(loss_pattern, 'start_date') else None,
            "qe_factors_count": len(qe_factors),
            "total_exposure_distribution": total_distribution,
            "distribution_sum": loss_pattern.check_distribution(),
            "qe_factors": qe_factors
        }
    
    def get_complete_pattern_analysis(self, coverage_id: int) -> Dict:
        """
        Get complete analysis including both writing pattern and derived loss incurred pattern.
        
        Args:
            coverage_id: The coverage ID to analyze
            
        Returns:
            Dictionary with complete pattern analysis
        """
        # Get writing pattern analysis
        writing_analysis = self.get_pattern_analysis(coverage_id)
        if 'error' in writing_analysis:
            return writing_analysis
        
        # Get loss incurred pattern analysis
        loss_analysis = self.get_loss_incurred_pattern_summary(coverage_id)
        
        return {
            "coverage_id": coverage_id,
            "writing_pattern": writing_analysis,
            "loss_incurred_pattern": loss_analysis
        }
    
    def get_pattern_summary(self, coverage_id: int) -> Dict:
        """
        Get a summary of the pattern for a given coverage ID.
        
        Returns:
            Dictionary with pattern summary information
        """
        pattern_obj = self.construct_pattern_object(coverage_id)
        if pattern_obj is None:
            return {"error": f"No pattern found for coverage_id: {coverage_id}"}
        
        # Calculate upfront and distributed totals from Pattern elements
        upfront_total = sum(
            element.up_front for element in pattern_obj.elements 
            if hasattr(element, 'up_front')
        )
        distributed_total = sum(
            element.distribution for element in pattern_obj.elements 
            if hasattr(element, 'distribution')
        )
        
        # Get factor details
        upfront_factors = [
            {
                'period': element.period if hasattr(element, 'period') else None,
                'factor_value': element.up_front,
                'duration': element.duration if hasattr(element, 'duration') else None,
                'factor_type': element.factor_type.name if hasattr(element, 'factor_type') else None
            }
            for element in pattern_obj.elements 
            if hasattr(element, 'up_front') and element.up_front > 0
        ]
        
        distributed_factors = [
            {
                'period': element.period if hasattr(element, 'period') else None,
                'factor_value': element.distribution,
                'duration': element.duration if hasattr(element, 'duration') else None,
                'factor_type': element.factor_type.name if hasattr(element, 'factor_type') else None
            }
            for element in pattern_obj.elements 
            if hasattr(element, 'distribution') and element.distribution > 0
        ]
        
        return {
            "coverage_id": coverage_id,
            "pattern_id": pattern_obj.pattern_id if hasattr(pattern_obj, 'pattern_id') else None,
            "pattern_name": pattern_obj.name,
            "pattern_start_date": pattern_obj.start_date.isoformat() if hasattr(pattern_obj, 'start_date') else None,
            "upfront_factors_count": len(upfront_factors),
            "distributed_factors_count": len(distributed_factors),
            "total_upfront_factor": upfront_total,
            "total_distributed_factor": distributed_total,
            "total_elements": len(pattern_obj.elements),
            "distribution_sum": pattern_obj.check_distribution(),
            "upfront_factors": upfront_factors,
            "distributed_factors": distributed_factors
        }
    
    def batch_process_coverages(self, coverage_ids: List[int]) -> List[Dict]:
        """
        Process multiple coverage IDs in batch for efficiency.
        
        Args:
            coverage_ids: List of coverage IDs to process
            
        Returns:
            List of pattern summaries
        """
        results = []
        for coverage_id in coverage_ids:
            summary = self.get_pattern_summary(coverage_id)
            results.append(summary)
        return results


def create_coverage_pattern_reader(spark: SparkSession = None, data_path: str = "scratch/gold") -> CoveragePatternReader:
    """
    Factory function to create a CoveragePatternReader.
    
    Args:
        spark: SparkSession instance (will create one if None)
        data_path: Path to the data files
        
    Returns:
        CoveragePatternReader instance
    """
    if spark is None:
        spark = SparkSession.builder.appName("CoveragePatternReader").getOrCreate()
    
    return CoveragePatternReader(spark, data_path)


# Example usage for Databricks
def example_usage():
    """Example of how to use the CoveragePatternReader in Databricks."""
    
    # In Databricks, you typically already have a spark session available
    # spark = SparkSession.builder.appName("CoveragePatternExample").getOrCreate()
    
    # Create the reader
    reader = create_coverage_pattern_reader()
    
    # Example 1: Get pattern for a specific coverage
    coverage_id = 100
    pattern_obj = reader.construct_pattern_object(coverage_id)
    
    if pattern_obj:
        print(f"Pattern Object: {pattern_obj.name} (ID: {getattr(pattern_obj, 'pattern_id', 'N/A')})")
        print(f"Total Elements: {len(pattern_obj.elements)}")
        print(f"Distribution Check: {pattern_obj.check_distribution()}")
        
        # Show upfront factors (QS)
        upfront_elements = [e for e in pattern_obj.elements if hasattr(e, 'up_front') and e.up_front > 0]
        print(f"Upfront (QS) Factors: {len(upfront_elements)}")
        for element in upfront_elements:
            print(f"  - Period: {getattr(element, 'period', 'N/A')}, "
                  f"Value: {element.up_front}, "
                  f"Type: {element.factor_type.name if hasattr(element, 'factor_type') else 'N/A'}")
        
        # Show distributed factors (Q)
        distributed_elements = [e for e in pattern_obj.elements if hasattr(e, 'distribution') and e.distribution > 0]
        print(f"Distributed (Q) Factors: {len(distributed_elements)}")
        for element in distributed_elements:
            print(f"  - Period: {getattr(element, 'period', 'N/A')}, "
                  f"Value: {element.distribution}, "
                  f"Type: {element.factor_type.name if hasattr(element, 'factor_type') else 'N/A'}")
    
    # Example 2: Get pattern summary
    summary = reader.get_pattern_summary(coverage_id)
    print(f"\nPattern Summary: {summary}")
    
    # Example 3: Batch process multiple coverages
    coverage_ids = [100, 101]
    batch_results = reader.batch_process_coverages(coverage_ids)
    for result in batch_results:
        print(f"\nCoverage {result.get('coverage_id')}: "
              f"Upfront Total: {result.get('total_upfront_factor', 0):.4f}, "
              f"Distributed Total: {result.get('total_distributed_factor', 0):.4f}")


def quick_test():
    """Quick test function for development."""
    print("Testing CoveragePatternReader...")
    
    try:
        reader = create_coverage_pattern_reader()
        
        # Test with a sample coverage ID
        test_coverage_id = 100
        print(f"Testing with coverage_id: {test_coverage_id}")
        
        # Get pattern ID
        pattern_id = reader.get_writing_premium_pattern_id(test_coverage_id)
        print(f"Found pattern_id: {pattern_id}")
        
        if pattern_id:
            # Construct pattern object
            pattern = reader.construct_pattern_object(test_coverage_id)
            if pattern:
                print(f"Successfully created Pattern: {pattern.name}")
                print(f"Pattern has {len(pattern.elements)} elements")
                
                # Get summary
                summary = reader.get_pattern_summary(test_coverage_id)
                print(f"Pattern Summary Keys: {list(summary.keys())}")
            else:
                print("Failed to create pattern object")
        else:
            print("No pattern ID found for coverage")
            
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # For local testing
    print("Running quick test...")
    quick_test()
    
    print("\nRunning example usage...")
    example_usage()
