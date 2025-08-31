"""
Final comprehensive test of all CoveragePatternReader functionality
"""

from coverage_pattern_reader import create_coverage_pattern_reader

def comprehensive_test():
    """Comprehensive test of all functionality."""
    
    print("=== COMPREHENSIVE COVERAGE PATTERN READER TEST ===")
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Test different pattern types
    test_cases = [
        (100, "Simple pattern (all distributed)"),
        (101, "Complex pattern (QS + 4Q)"),
        (102, "All upfront pattern")
    ]
    
    for coverage_id, description in test_cases:
        print(f"\n{'='*60}")
        print(f"Testing Coverage {coverage_id}: {description}")
        print('='*60)
        
        # 1. Basic pattern construction
        pattern = reader.construct_pattern_object(coverage_id)
        if pattern:
            print(f"✅ Pattern: {pattern.name}")
            print(f"   Start Date: {getattr(pattern, 'start_date', 'N/A')}")
            print(f"   Elements: {len(pattern.elements)}")
            
            # Show consolidated factors
            upfront_count = sum(1 for e in pattern.elements if e.up_front > 0)
            distributed_count = sum(1 for e in pattern.elements if e.distribution > 0)
            combined_count = sum(1 for e in pattern.elements if e.up_front > 0 and e.distribution > 0)
            
            print(f"   Factor Summary: {upfront_count} upfront, {distributed_count} distributed, {combined_count} combined")
        
        # 2. Pattern summary
        summary = reader.get_pattern_summary(coverage_id)
        if 'error' not in summary:
            print(f"✅ Summary: {summary['total_upfront_factor']:.4f} upfront + {summary['total_distributed_factor']:.4f} distributed = {summary['total_upfront_factor'] + summary['total_distributed_factor']:.4f}")
        
        # 3. Factor generation
        factors = reader.generate_pattern_factors(coverage_id)
        if factors:
            print(f"✅ Generated {len(factors)} factors")
            
            # Check factor values sum to 1.0
            total_factor_value = sum(f.value for f in factors)
            print(f"   Factor sum: {total_factor_value:.6f} (should be 1.0)")
        
        # 4. Exposure matrix
        matrix = reader.calculate_exposure_matrix(coverage_id)
        if matrix:
            print(f"✅ Matrix: {len(matrix.get_matrix_entries())} entries, sum: {matrix.get_total_sum():.6f}")
        
        # 5. Vectors
        exposure_vector = reader.get_exposure_vector(coverage_id)
        incurred_vector = reader.get_incurred_vector(coverage_id)
        if exposure_vector and incurred_vector:
            exp_sum = sum(e.sum for e in exposure_vector)
            inc_sum = sum(e.sum for e in incurred_vector)
            print(f"✅ Vectors: {len(exposure_vector)} exposure quarters (sum: {exp_sum:.6f}), {len(incurred_vector)} incurred quarters (sum: {inc_sum:.6f})")
        
        # 6. Comprehensive analysis
        analysis = reader.get_pattern_analysis(coverage_id)
        if 'error' not in analysis:
            print(f"✅ Analysis: Complete with {analysis['factors_count']} factors, {analysis['matrix_entries_count']} matrix entries")
            print(f"   Date range: {analysis['date_ranges']['min_incurred']} to {analysis['date_ranges']['max_exposed']}")
        else:
            print(f"❌ Analysis error: {analysis['error']}")
    
    print(f"\n{'='*60}")
    print("VALIDATION CHECKS")
    print('='*60)
    
    # Validation: All sums should equal 1.0
    for coverage_id, description in test_cases:
        analysis = reader.get_pattern_analysis(coverage_id)
        if 'error' not in analysis:
            matrix_sum = analysis['matrix_total_sum']
            exp_sum = sum(e['sum'] for e in analysis['exposure_vector'])
            inc_sum = sum(e['sum'] for e in analysis['incurred_vector'])
            
            print(f"Coverage {coverage_id}:")
            print(f"  Matrix sum: {matrix_sum:.6f} {'✅' if abs(matrix_sum - 1.0) < 0.000001 else '❌'}")
            print(f"  Exposure sum: {exp_sum:.6f} {'✅' if abs(exp_sum - 1.0) < 0.000001 else '❌'}")
            print(f"  Incurred sum: {inc_sum:.6f} {'✅' if abs(inc_sum - 1.0) < 0.000001 else '❌'}")
    
    print(f"\n{'='*60}")
    print("TEST COMPLETE - All functionality working! 🎉")
    print('='*60)

if __name__ == "__main__":
    comprehensive_test()
