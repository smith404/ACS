"""
Databricks Usage Example for Coverage Pattern Reader
This script demonstrates how to use the CoveragePatternReader in a Databricks environment.
"""

from coverage_pattern_reader import create_coverage_pattern_reader

def databricks_example():
    """
    Example usage for Databricks notebooks or scripts.
    In Databricks, the SparkSession is typically already available as 'spark'.
    """
    
    print("=== Databricks Coverage Pattern Reader Example ===")
    
    # In Databricks, you would typically use the existing spark session:
    # reader = create_coverage_pattern_reader(spark, "scratch/gold")
    
    # For this example, we'll create our own session
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Example 1: Process a single coverage
    print("\n1. Single Coverage Pattern Analysis:")
    coverage_id = 100
    pattern = reader.construct_pattern_object(coverage_id)
    
    if pattern:
        print(f"   Coverage ID: {coverage_id}")
        print(f"   Pattern Name: {pattern.name}")
        print(f"   Pattern ID: {getattr(pattern, 'pattern_id', 'N/A')}")
        print(f"   Total Elements: {len(pattern.elements)}")
        
        # Calculate totals
        upfront_total = sum(e.up_front for e in pattern.elements)
        distributed_total = sum(e.distribution for e in pattern.elements)
        
        print(f"   Upfront Total: {upfront_total:.4f}")
        print(f"   Distributed Total: {distributed_total:.4f}")
        print(f"   Distribution Sum Valid: {pattern.check_distribution()}")
    
    # Example 2: Process multiple coverages in batch
    print("\n2. Batch Coverage Processing:")
    coverage_ids = [100, 101]
    batch_results = reader.batch_process_coverages(coverage_ids)
    
    for result in batch_results:
        if 'error' not in result:
            print(f"   Coverage {result['coverage_id']}: "
                  f"Upfront={result['total_upfront_factor']:.4f}, "
                  f"Distributed={result['total_distributed_factor']:.4f}, "
                  f"Pattern='{result['pattern_name']}'")
        else:
            print(f"   Error for coverage {result.get('coverage_id', 'Unknown')}: {result['error']}")
    
    # Example 3: NEW - Generate pattern factors and exposure matrix
    print("\n3. NEW - Pattern Factors and Exposure Matrix Analysis for Coverage 101:")
    
    # Generate factors using pattern start date
    factors = reader.generate_pattern_factors(101)
    if factors:
        print(f"   Generated Factors: {len(factors)}")
        print(f"   Sample factors:")
        for i, factor in enumerate(factors[:3]):
            print(f"      Factor {i+1}: incurred={factor.incurred_date}, exposed={factor.exposed_date}, value={factor.value:.6f}")
    
    # Calculate exposure matrix
    matrix = reader.calculate_exposure_matrix(101)
    if matrix:
        print(f"   Exposure Matrix: {len(matrix.get_matrix_entries())} entries, Total: {matrix.get_total_sum():.6f}")
        
        # Get exposure vector (financial quarters)
        exposure_vector = reader.get_exposure_vector(101)
        print(f"   Exposure Vector (Financial Quarters): {len(exposure_vector)} quarters")
        for entry in exposure_vector[:5]:  # Show first 5
            print(f"      {entry.date_bucket}: {entry.sum:.6f}")
        
        # Get incurred vector  
        incurred_vector = reader.get_incurred_vector(101)
        print(f"   Incurred Vector: {len(incurred_vector)} quarters")
        for entry in incurred_vector:
            print(f"      {entry.date_bucket}: {entry.sum:.6f}")
    
    # Example 4: Comprehensive pattern analysis
    print("\n4. NEW - Comprehensive Pattern Analysis:")
    analysis = reader.get_pattern_analysis(101)
    if 'error' not in analysis:
        print(f"   Pattern: {analysis['pattern_name']} (Start: {analysis['pattern_start_date']})")
        print(f"   Pattern Elements: {analysis['total_elements']}")
        print(f"   Generated Factors: {analysis['factors_count']}")
        print(f"   Matrix Summary: {analysis['matrix_entries_count']} entries, Total: {analysis['matrix_total_sum']:.6f}")
        print(f"   Vectors: {analysis['exposure_vector_count']} exposure, {analysis['incurred_vector_count']} incurred")
        print(f"   Date Range: {analysis['date_ranges']['min_exposed']} to {analysis['date_ranges']['max_exposed']}")
    
    # Example 5: OLD - Detailed factor analysis (kept for compatibility)
    print("\n5. Detailed Factor Analysis for Coverage 101 (Previous functionality):")
    summary = reader.get_pattern_summary(101)
    
    if 'error' not in summary:
        print(f"   Pattern: {summary['pattern_name']}")
        print(f"   Upfront Factors ({summary['upfront_factors_count']}):")
        for factor in summary['upfront_factors']:
            print(f"      Period {factor['period']}: {factor['factor_value']:.4f} "
                  f"({factor['factor_type']}, Duration: {factor['duration']})")
        
        print(f"   Distributed Factors ({summary['distributed_factors_count']}):")
        for factor in summary['distributed_factors']:
            print(f"      Period {factor['period']}: {factor['factor_value']:.4f} "
                  f"({factor['factor_type']}, Duration: {factor['duration']})")
    
    # Example 6: NEW - Loss Incurred Pattern from Exposure Vector
    print("\n6. NEW - Loss Incurred Pattern Analysis for Coverage 101:")
    
    # Create loss incurred pattern from exposure vector
    loss_pattern = reader.create_loss_incurred_pattern(101)
    if loss_pattern:
        print(f"   Loss Pattern: {loss_pattern.name}")
        print(f"   Loss Pattern ID: {getattr(loss_pattern, 'pattern_id', 'N/A')}")
        print(f"   Loss Start Date: {getattr(loss_pattern, 'start_date', 'N/A')}")
        print(f"   QE Factors: {len(loss_pattern.elements)}")
        
        # Show QE factors
        print(f"   First 3 QE (Quarter Exposure) Factors:")
        for i, element in enumerate(loss_pattern.elements[:3]):
            exposure_date = getattr(element, 'exposure_date', 'N/A')
            duration = getattr(element, 'duration', 0)
            period_freq = getattr(element, 'period_frequency', 'QE')
            print(f"      QE{i+1}: {exposure_date} → {element.distribution:.6f} (duration: {duration}, type: {period_freq})")
        
        total_qe = sum(e.distribution for e in loss_pattern.elements)
        print(f"   Total QE Distribution: {total_qe:.6f}")
    
    # Get loss pattern summary
    loss_summary = reader.get_loss_incurred_pattern_summary(101)
    if 'error' not in loss_summary:
        print(f"   Loss Pattern Summary: {loss_summary['qe_factors_count']} QE factors, Distribution Valid: {loss_summary['distribution_sum']}")
    
    # Example 7: Complete analysis (both patterns)
    print("\n7. NEW - Complete Pattern Analysis (Writing + Loss Incurred):")
    complete_analysis = reader.get_complete_pattern_analysis(101)
    if 'error' not in complete_analysis:
        writing = complete_analysis['writing_pattern']
        loss = complete_analysis['loss_incurred_pattern']
        
        print(f"   Writing Pattern: {writing['pattern_name']}")
        print(f"     - Start Date: {writing['pattern_start_date']}")
        print(f"     - Exposure Vector: {writing['exposure_vector_count']} quarters")
        print(f"     - Matrix Total: {writing['matrix_total_sum']:.6f}")
        
        print(f"   Loss Incurred Pattern: {loss['loss_pattern_name']}")
        print(f"     - Start Date: {loss['loss_pattern_start_date']}")
        print(f"     - QE Factors: {loss['qe_factors_count']}")
        print(f"     - Total Distribution: {loss['total_exposure_distribution']:.6f}")

    print("\n=== Example Complete ===")

    # Test the new functions
    print("\n=== Testing New Databricks Functions ===")
    
    # Test loss incurred pattern function
    loss_pattern_direct = get_loss_incurred_pattern_for_coverage(101)
    if loss_pattern_direct:
        print(f"Direct Loss Pattern: {loss_pattern_direct.name} with {len(loss_pattern_direct.elements)} QE factors")
    
    # Test both patterns analysis function
    both_patterns = get_both_patterns_analysis(101)
    if 'error' not in both_patterns:
        print(f"Both Patterns Analysis: Writing → {both_patterns['writing_pattern']['exposure_vector_count']} quarters, Loss → {both_patterns['loss_incurred_pattern']['qe_factors_count']} QE factors")

def simple_lookup_function(coverage_id: int) -> dict:
    """
    Simple function to lookup pattern information for a coverage ID.
    This is the main function you would use in Databricks.
    """
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    return reader.get_pattern_summary(coverage_id)

# For Databricks notebook usage:
def get_pattern_for_coverage(coverage_id: int):
    """
    Main function for Databricks - returns a Pattern object for the given coverage ID.
    
    Usage in Databricks:
        from coverage_pattern_reader import get_pattern_for_coverage
        pattern = get_pattern_for_coverage(100)
        print(f"Pattern: {pattern.name}")
        print(f"Elements: {len(pattern.elements)}")
    """
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    return reader.construct_pattern_object(coverage_id)

def get_exposure_analysis_for_coverage(coverage_id: int):
    """
    Enhanced function for Databricks - returns comprehensive exposure analysis.
    
    Usage in Databricks:
        from coverage_pattern_reader import get_exposure_analysis_for_coverage
        analysis = get_exposure_analysis_for_coverage(101)
        print(f"Exposure Vector: {len(analysis['exposure_vector'])} quarters")
        for entry in analysis['exposure_vector']:
            print(f"Quarter {entry['date_bucket']}: {entry['sum']:.6f}")
    """
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    return reader.get_pattern_analysis(coverage_id)

def get_loss_incurred_pattern_for_coverage(coverage_id: int):
    """
    Function for Databricks - returns loss incurred pattern derived from exposure vector.
    
    Usage in Databricks:
        from coverage_pattern_reader import get_loss_incurred_pattern_for_coverage
        loss_pattern = get_loss_incurred_pattern_for_coverage(101)
        print(f"Loss Pattern: {loss_pattern.name}")
        print(f"QE Factors: {len(loss_pattern.elements)}")
        for element in loss_pattern.elements:
            print(f"QE: {element.exposure_date} → {element.distribution:.6f}")
    """
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    return reader.create_loss_incurred_pattern(coverage_id)

def get_both_patterns_analysis(coverage_id: int):
    """
    Function for Databricks - returns analysis of both writing and loss incurred patterns.
    
    Usage in Databricks:
        from coverage_pattern_reader import get_both_patterns_analysis
        analysis = get_both_patterns_analysis(101)
        writing = analysis['writing_pattern']
        loss = analysis['loss_incurred_pattern']
        print(f"Writing: {writing['pattern_name']} → {writing['exposure_vector_count']} quarters")
        print(f"Loss: {loss['loss_pattern_name']} → {loss['qe_factors_count']} QE factors")
    """
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    return reader.get_complete_pattern_analysis(coverage_id)

if __name__ == "__main__":
    databricks_example()
    
    # Test the simple lookup
    print("\n=== Simple Lookup Test ===")
    result = simple_lookup_function(101)
    print(f"Simple lookup result keys: {list(result.keys())}")
    
    # Test the new exposure analysis
    print("\n=== Exposure Analysis Test ===")
    exposure_analysis = get_exposure_analysis_for_coverage(101)
    if 'error' not in exposure_analysis:
        print(f"Exposure analysis for coverage 101:")
        print(f"  Start Date: {exposure_analysis['pattern_start_date']}")
        print(f"  Factors Generated: {exposure_analysis['factors_count']}")
        print(f"  Matrix Total: {exposure_analysis['matrix_total_sum']:.6f}")
        print(f"  Exposure Vector Quarters: {exposure_analysis['exposure_vector_count']}")
        print(f"  Sample exposure quarters:")
        for entry in exposure_analysis['exposure_vector'][:3]:
            print(f"    {entry['date_bucket']}: {entry['sum']:.6f}")
    else:
        print(f"Error: {exposure_analysis['error']}")
