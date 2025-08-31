"""
Test script for new exposure matrix functionality in CoveragePatternReader
"""

from coverage_pattern_reader import create_coverage_pattern_reader

def test_exposure_matrix_functionality():
    """Test the new exposure matrix functionality."""
    
    print("=== Testing Exposure Matrix Functionality ===")
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Test with coverage 101 which has a complex pattern
    coverage_id = 101
    print(f"\nTesting with coverage_id: {coverage_id}")
    
    # 1. Test pattern factor generation
    print("\n1. Generating Pattern Factors:")
    factors = reader.generate_pattern_factors(coverage_id)
    if factors:
        print(f"   Generated {len(factors)} factors")
        print(f"   First 3 factors:")
        for i, factor in enumerate(factors[:3]):
            print(f"      Factor {i+1}: incurred={factor.incurred_date}, exposed={factor.exposed_date}, value={factor.value:.6f}")
    else:
        print("   Failed to generate factors")
        return
    
    # 2. Test exposure matrix calculation
    print("\n2. Calculating Exposure Matrix:")
    matrix = reader.calculate_exposure_matrix(coverage_id)
    if matrix:
        print(f"   Matrix entries: {len(matrix.get_matrix_entries())}")
        print(f"   Total sum: {matrix.get_total_sum():.6f}")
        
        # Show matrix table (limited rows for readability)
        print(f"   Matrix table preview:")
        table_lines = matrix.format_matrix_table(precision=6).split('\n')
        for line in table_lines[:10]:  # Show first 10 lines
            print(f"      {line}")
        if len(table_lines) > 10:
            print(f"      ... ({len(table_lines)-10} more lines)")
    else:
        print("   Failed to calculate matrix")
        return
    
    # 3. Test exposure vector
    print("\n3. Getting Exposure Vector:")
    exposure_vector = reader.get_exposure_vector(coverage_id)
    if exposure_vector:
        print(f"   Exposure vector entries: {len(exposure_vector)}")
        print(f"   First 5 entries:")
        for i, entry in enumerate(exposure_vector[:5]):
            print(f"      {entry.date_bucket}: {entry.sum:.6f}")
        
        total_exposure = sum(entry.sum for entry in exposure_vector)
        print(f"   Total exposure: {total_exposure:.6f}")
    else:
        print("   Failed to get exposure vector")
    
    # 4. Test incurred vector
    print("\n4. Getting Incurred Vector:")
    incurred_vector = reader.get_incurred_vector(coverage_id)
    if incurred_vector:
        print(f"   Incurred vector entries: {len(incurred_vector)}")
        print(f"   First 5 entries:")
        for i, entry in enumerate(incurred_vector[:5]):
            print(f"      {entry.date_bucket}: {entry.sum:.6f}")
        
        total_incurred = sum(entry.sum for entry in incurred_vector)
        print(f"   Total incurred: {total_incurred:.6f}")
    else:
        print("   Failed to get incurred vector")
    
    # 5. Test comprehensive analysis
    print("\n5. Getting Comprehensive Pattern Analysis:")
    analysis = reader.get_pattern_analysis(coverage_id)
    if 'error' not in analysis:
        print(f"   Pattern: {analysis['pattern_name']}")
        print(f"   Start Date: {analysis['pattern_start_date']}")
        print(f"   Pattern Elements: {analysis['total_elements']}")
        print(f"   Generated Factors: {analysis['factors_count']}")
        print(f"   Matrix Entries: {analysis['matrix_entries_count']}")
        print(f"   Matrix Total: {analysis['matrix_total_sum']:.6f}")
        print(f"   Exposure Vector Length: {analysis['exposure_vector_count']}")
        print(f"   Incurred Vector Length: {analysis['incurred_vector_count']}")
        print(f"   Date Ranges:")
        for key, value in analysis['date_ranges'].items():
            print(f"      {key}: {value}")
        
        print(f"   Exposure Vector Preview (first 3):")
        for entry in analysis['exposure_vector'][:3]:
            print(f"      {entry['date_bucket']}: {entry['sum']:.6f}")
    else:
        print(f"   Error: {analysis['error']}")

def test_multiple_coverages():
    """Test with multiple coverages to compare patterns."""
    
    print("\n\n=== Testing Multiple Coverages ===")
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    coverage_ids = [100, 101, 102]
    
    for coverage_id in coverage_ids:
        print(f"\nCoverage {coverage_id}:")
        analysis = reader.get_pattern_analysis(coverage_id)
        
        if 'error' not in analysis:
            print(f"  Pattern: {analysis['pattern_name']}")
            print(f"  Start Date: {analysis['pattern_start_date']}")
            print(f"  Factors: {analysis['factors_count']}")
            print(f"  Matrix Total: {analysis['matrix_total_sum']:.6f}")
            print(f"  Exposure Vector: {analysis['exposure_vector_count']} quarters")
        else:
            print(f"  Error: {analysis['error']}")

if __name__ == "__main__":
    test_exposure_matrix_functionality()
    test_multiple_coverages()
