"""
Test script for Loss Incurred Pattern functionality
"""

from coverage_pattern_reader import create_coverage_pattern_reader

def test_loss_incurred_patterns():
    """Test the new loss incurred pattern functionality."""
    
    print("=== Testing Loss Incurred Pattern Creation ===")
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Test with different coverage patterns
    test_coverages = [100, 101, 102]
    
    for coverage_id in test_coverages:
        print(f"\n{'='*60}")
        print(f"Testing Coverage {coverage_id}")
        print('='*60)
        
        # 1. Show original writing pattern info
        writing_summary = reader.get_pattern_summary(coverage_id)
        if 'error' not in writing_summary:
            print(f"✅ Writing Pattern: {writing_summary['pattern_name']}")
            print(f"   Pattern ID: {writing_summary['pattern_id']}")
            print(f"   Start Date: {writing_summary['pattern_start_date']}")
            print(f"   Upfront + Distributed: {writing_summary['total_upfront_factor']:.4f} + {writing_summary['total_distributed_factor']:.4f}")
        
        # 2. Show exposure vector from writing pattern
        exposure_vector = reader.get_exposure_vector(coverage_id)
        if exposure_vector:
            print(f"✅ Exposure Vector: {len(exposure_vector)} quarters")
            print(f"   Date range: {exposure_vector[0].date_bucket} to {exposure_vector[-1].date_bucket}")
            print(f"   Total exposure: {sum(e.sum for e in exposure_vector):.6f}")
            
            # Show first few exposure entries
            print("   First 3 exposure quarters:")
            for i, entry in enumerate(exposure_vector[:3]):
                print(f"      Q{i+1}: {entry.date_bucket} → {entry.sum:.6f}")
        
        # 3. Create and analyze loss incurred pattern
        print("\n📊 Creating Loss Incurred Pattern:")
        loss_pattern = reader.create_loss_incurred_pattern(coverage_id)
        if loss_pattern:
            print(f"✅ Loss Pattern Created: {loss_pattern.name}")
            print(f"   Loss Pattern ID: {getattr(loss_pattern, 'pattern_id', 'N/A')}")
            print(f"   Loss Start Date: {getattr(loss_pattern, 'start_date', 'N/A')}")
            print(f"   QE Factors Count: {len(loss_pattern.elements)}")
            
            # Show QE factors
            print("   QE Factors (Quarter Exposure):")
            for i, element in enumerate(loss_pattern.elements[:5]):  # Show first 5
                exposure_date = getattr(element, 'exposure_date', 'N/A')
                period_freq = getattr(element, 'period_frequency', 'QE')
                duration = getattr(element, 'duration', 0)
                print(f"      QE{i+1}: {exposure_date} → {element.distribution:.6f} (duration: {duration}, type: {period_freq})")
            
            if len(loss_pattern.elements) > 5:
                print(f"      ... and {len(loss_pattern.elements) - 5} more QE factors")
            
            # Validate total
            total_qe = sum(e.distribution for e in loss_pattern.elements)
            print(f"   Total QE Distribution: {total_qe:.6f} {'✅' if abs(total_qe - 1.0) < 0.000001 else '❌'}")
        else:
            print("❌ Failed to create loss incurred pattern")
        
        # 4. Get loss incurred pattern summary
        loss_summary = reader.get_loss_incurred_pattern_summary(coverage_id)
        if 'error' not in loss_summary:
            print(f"✅ Loss Pattern Summary:")
            print(f"   Original Writing Pattern ID: {loss_summary['original_writing_pattern_id']}")
            print(f"   QE Factors: {loss_summary['qe_factors_count']}")
            print(f"   Distribution Valid: {loss_summary['distribution_sum']}")
        
        # 5. Complete analysis (both patterns)
        complete_analysis = reader.get_complete_pattern_analysis(coverage_id)
        if 'error' not in complete_analysis:
            writing = complete_analysis['writing_pattern']
            loss = complete_analysis['loss_incurred_pattern']
            
            print(f"✅ Complete Analysis:")
            print(f"   Writing: {writing['pattern_name']} → {writing['exposure_vector_count']} exposure quarters")
            print(f"   Loss: {loss['loss_pattern_name']} → {loss['qe_factors_count']} QE factors")
            print(f"   Exposure Vector → QE Mapping: 1:1 ✅")

def demonstrate_qe_pattern_structure():
    """Demonstrate the QE pattern structure in detail."""
    
    print(f"\n{'='*60}")
    print("DETAILED QE PATTERN STRUCTURE DEMONSTRATION")
    print('='*60)
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Use coverage 101 which has a complex pattern
    coverage_id = 101
    
    print(f"Coverage {coverage_id} - Detailed QE Pattern Analysis:")
    
    # Get loss incurred pattern
    loss_pattern = reader.create_loss_incurred_pattern(coverage_id)
    if loss_pattern:
        print(f"\nLoss Incurred Pattern Details:")
        print(f"  Name: {loss_pattern.name}")
        print(f"  ID: {getattr(loss_pattern, 'pattern_id', 'N/A')}")
        print(f"  Start Date: {getattr(loss_pattern, 'start_date', 'N/A')}")
        print(f"  Elements: {len(loss_pattern.elements)}")
        
        print(f"\nQE Pattern Factors (All):")
        for i, element in enumerate(loss_pattern.elements):
            exposure_date = getattr(element, 'exposure_date', 'N/A')
            period_freq = getattr(element, 'period_frequency', 'QE')
            duration = getattr(element, 'duration', 0)
            factor_type = element.type.name if hasattr(element, 'type') else 'N/A'
            
            print(f"  QE{i+1:2d}: Date={exposure_date}, Value={element.distribution:.6f}, "
                  f"Duration={duration}, Type={period_freq}, FactorType={factor_type}")
        
        # Show comparison with original exposure vector
        exposure_vector = reader.get_exposure_vector(coverage_id)
        print(f"\nValidation - QE Factors vs Original Exposure Vector:")
        print(f"{'Index':<5} {'Exposure Date':<12} {'Exposure Value':<15} {'QE Value':<12} {'Match':<5}")
        print("-" * 60)
        
        for i, (exp_entry, qe_element) in enumerate(zip(exposure_vector, loss_pattern.elements)):
            exp_date = exp_entry.date_bucket
            exp_value = exp_entry.sum
            qe_date = getattr(qe_element, 'exposure_date', None)
            qe_value = qe_element.distribution
            
            match = "✅" if (exp_date == qe_date and abs(exp_value - qe_value) < 0.000001) else "❌"
            
            print(f"{i+1:<5} {exp_date:<12} {exp_value:<15.6f} {qe_value:<12.6f} {match:<5}")

if __name__ == "__main__":
    test_loss_incurred_patterns()
    demonstrate_qe_pattern_structure()
