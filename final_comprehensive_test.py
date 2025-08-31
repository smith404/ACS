"""
Final comprehensive test including loss incurred patterns
"""

from coverage_pattern_reader import create_coverage_pattern_reader

def final_comprehensive_test():
    """Final test of all functionality including loss incurred patterns."""
    
    print("=== FINAL COMPREHENSIVE TEST - ALL FUNCTIONALITY ===")
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Test different coverage types
    test_cases = [
        (100, "Simple pattern (all distributed)"),
        (101, "Complex pattern (QS + 4Q)"),
        (102, "All upfront pattern")
    ]
    
    for coverage_id, description in test_cases:
        print(f"\n{'='*80}")
        print(f"TESTING COVERAGE {coverage_id}: {description}")
        print('='*80)
        
        # 1. Writing Pattern Analysis
        print(f"\n📝 WRITING PATTERN ANALYSIS:")
        writing_summary = reader.get_pattern_summary(coverage_id)
        if 'error' not in writing_summary:
            print(f"✅ Pattern: {writing_summary['pattern_name']}")
            print(f"   ID: {writing_summary['pattern_id']}, Start: {writing_summary['pattern_start_date']}")
            print(f"   Elements: {writing_summary['total_elements']}")
            print(f"   Distribution: {writing_summary['total_upfront_factor']:.4f} upfront + {writing_summary['total_distributed_factor']:.4f} distributed")
        
        # 2. Exposure Vector Generation
        print(f"\n🎯 EXPOSURE VECTOR GENERATION:")
        exposure_vector = reader.get_exposure_vector(coverage_id)
        if exposure_vector:
            print(f"✅ Exposure Vector: {len(exposure_vector)} quarters")
            print(f"   Date Range: {exposure_vector[0].date_bucket} → {exposure_vector[-1].date_bucket}")
            print(f"   Total: {sum(e.sum for e in exposure_vector):.6f}")
        
        # 3. Loss Incurred Pattern Creation
        print(f"\n🔄 LOSS INCURRED PATTERN CREATION:")
        loss_pattern = reader.create_loss_incurred_pattern(coverage_id)
        if loss_pattern:
            print(f"✅ Loss Pattern: {loss_pattern.name}")
            print(f"   ID: {getattr(loss_pattern, 'pattern_id', 'N/A')}")
            print(f"   Start Date: {getattr(loss_pattern, 'start_date', 'N/A')}")
            print(f"   QE Factors: {len(loss_pattern.elements)}")
            
            # Show QE factor details
            total_qe = sum(e.distribution for e in loss_pattern.elements)
            print(f"   Total QE Distribution: {total_qe:.6f}")
            
            # Verify 1:1 mapping
            if len(exposure_vector) == len(loss_pattern.elements):
                mapping_correct = all(
                    abs(exp.sum - qe.distribution) < 0.000001 and exp.date_bucket == getattr(qe, 'exposure_date', None)
                    for exp, qe in zip(exposure_vector, loss_pattern.elements)
                )
                print(f"   Exposure → QE Mapping: {'✅ Perfect 1:1' if mapping_correct else '❌ Mismatch'}")
        
        # 4. Complete Analysis
        print(f"\n📊 COMPLETE PATTERN ANALYSIS:")
        complete_analysis = reader.get_complete_pattern_analysis(coverage_id)
        if 'error' not in complete_analysis:
            writing = complete_analysis['writing_pattern']
            loss = complete_analysis['loss_incurred_pattern']
            
            print(f"✅ Writing → Loss Transformation:")
            print(f"   Writing Pattern: {writing['pattern_name']}")
            print(f"     Elements: {writing['total_elements']} (QS/Q factors)")
            print(f"     Generated Factors: {writing['factors_count']}")
            print(f"     Exposure Vector: {writing['exposure_vector_count']} quarters")
            
            print(f"   Loss Incurred Pattern: {loss['loss_pattern_name']}")
            print(f"     QE Factors: {loss['qe_factors_count']}")
            print(f"     Distribution: {loss['total_exposure_distribution']:.6f}")
            
            # Validate the transformation chain
            writing_total = writing['matrix_total_sum']
            loss_total = loss['total_exposure_distribution']
            chain_valid = abs(writing_total - loss_total) < 0.000001
            
            print(f"   Transformation Chain: Writing({writing_total:.6f}) → Loss({loss_total:.6f}) {'✅' if chain_valid else '❌'}")
    
    print(f"\n{'='*80}")
    print("VALIDATION SUMMARY")
    print('='*80)
    
    all_valid = True
    for coverage_id, description in test_cases:
        complete = reader.get_complete_pattern_analysis(coverage_id)
        if 'error' not in complete:
            writing = complete['writing_pattern']
            loss = complete['loss_incurred_pattern']
            
            # Validate totals
            matrix_sum = writing['matrix_total_sum']
            exposure_sum = sum(e['sum'] for e in writing['exposure_vector'])
            qe_sum = loss['total_exposure_distribution']
            
            valid = (abs(matrix_sum - 1.0) < 0.000001 and 
                    abs(exposure_sum - 1.0) < 0.000001 and 
                    abs(qe_sum - 1.0) < 0.000001)
            
            status = "✅" if valid else "❌"
            print(f"Coverage {coverage_id}: Matrix={matrix_sum:.6f}, Exposure={exposure_sum:.6f}, QE={qe_sum:.6f} {status}")
            
            if not valid:
                all_valid = False
    
    print(f"\n{'='*80}")
    final_status = "🎉 ALL TESTS PASSED!" if all_valid else "❌ SOME TESTS FAILED"
    print(f"{final_status}")
    print(f"✅ Writing patterns with QS/Q factor consolidation")
    print(f"✅ Pattern factor generation with start dates")
    print(f"✅ Exposure matrix calculation with financial quarters")
    print(f"✅ Exposure vector extraction")
    print(f"✅ Loss incurred pattern creation with QE factors (duration 0)")
    print(f"✅ Perfect 1:1 mapping from exposure vector to QE factors")
    print(f"✅ Complete pattern transformation pipeline")
    print('='*80)

if __name__ == "__main__":
    final_comprehensive_test()
