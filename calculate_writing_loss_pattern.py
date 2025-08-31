"""
Calculate Writing Loss Pattern for a given Coverage ID

This script takes a coverage ID as a parameter, finds the writing_premium_pattern_id,
calculates the writing loss pattern (loss incurred pattern), and prints out the factor table.
"""

import sys
from coverage_pattern_reader import create_coverage_pattern_reader
from typing import Optional

def print_factor_table(coverage_id: int, data_path: str = "scratch/gold"):
    """
    Calculate and print the writing loss pattern factor table for a coverage ID.
    
    Args:
        coverage_id: The coverage ID to process
        data_path: Path to the data directory
    """
    
    print(f"{'='*80}")
    print(f"WRITING LOSS PATTERN CALCULATION FOR COVERAGE {coverage_id}")
    print(f"{'='*80}")
    
    try:
        # Initialize the pattern reader
        reader = create_coverage_pattern_reader(data_path=data_path)
        
        # Step 1: Get writing premium pattern information
        print(f"\n📋 STEP 1: WRITING PREMIUM PATTERN LOOKUP")
        print(f"{'='*50}")
        
        writing_summary = reader.get_pattern_summary(coverage_id)
        if 'error' in writing_summary:
            print(f"❌ Error: {writing_summary['error']}")
            return
        
        print(f"✅ Writing Premium Pattern Found:")
        print(f"   Coverage ID: {coverage_id}")
        print(f"   Writing Pattern ID: {writing_summary['pattern_id']}")
        print(f"   Pattern Name: {writing_summary['pattern_name']}")
        print(f"   Pattern Start Date: {writing_summary['pattern_start_date']}")
        print(f"   Total Elements: {writing_summary['total_elements']}")
        print(f"   Upfront Factor: {writing_summary['total_upfront_factor']:.6f}")
        print(f"   Distributed Factor: {writing_summary['total_distributed_factor']:.6f}")
        
        # Step 2: Calculate exposure vector
        print(f"\n🎯 STEP 2: EXPOSURE VECTOR CALCULATION")
        print(f"{'='*50}")
        
        exposure_vector = reader.get_exposure_vector(coverage_id)
        if not exposure_vector:
            print("❌ Failed to calculate exposure vector")
            return
        
        print(f"✅ Exposure Vector Generated:")
        print(f"   Total Quarters: {len(exposure_vector)}")
        print(f"   Date Range: {exposure_vector[0].date_bucket} → {exposure_vector[-1].date_bucket}")
        print(f"   Total Exposure: {sum(e.sum for e in exposure_vector):.6f}")
        
        # Step 3: Create writing loss pattern
        print(f"\n🔄 STEP 3: WRITING LOSS PATTERN CREATION")
        print(f"{'='*50}")
        
        loss_pattern = reader.create_loss_incurred_pattern(coverage_id)
        if not loss_pattern:
            print("❌ Failed to create writing loss pattern")
            return
        
        print(f"✅ Writing Loss Pattern Created:")
        print(f"   Loss Pattern Name: {loss_pattern.name}")
        print(f"   Loss Pattern ID: {getattr(loss_pattern, 'pattern_id', 'N/A')}")
        print(f"   Loss Start Date: {getattr(loss_pattern, 'start_date', 'N/A')}")
        print(f"   QE Factors Count: {len(loss_pattern.elements)}")
        
        # Step 4: Print detailed factor table
        print(f"\n📊 STEP 4: WRITING LOSS PATTERN FACTOR TABLE")
        print(f"{'='*50}")
        
        print_detailed_factor_table(exposure_vector, loss_pattern)
        
        # Step 5: Validation summary
        print(f"\n✅ VALIDATION SUMMARY")
        print(f"{'='*50}")
        
        total_exposure = sum(e.sum for e in exposure_vector)
        total_qe = sum(e.distribution for e in loss_pattern.elements)
        mapping_valid = len(exposure_vector) == len(loss_pattern.elements)
        
        print(f"   Exposure Vector Total: {total_exposure:.6f}")
        print(f"   QE Factors Total: {total_qe:.6f}")
        print(f"   1:1 Mapping: {'✅ Valid' if mapping_valid else '❌ Invalid'}")
        print(f"   Sum Validation: {'✅ Valid' if abs(total_qe - 1.0) < 0.000001 else '❌ Invalid'}")
        
        if mapping_valid and abs(total_qe - 1.0) < 0.000001:
            print(f"\n🎉 Writing Loss Pattern Successfully Calculated!")
        else:
            print(f"\n❌ Writing Loss Pattern Calculation Issues Detected!")
        
    except Exception as e:
        print(f"❌ Error during calculation: {str(e)}")
        import traceback
        traceback.print_exc()

def print_detailed_factor_table(exposure_vector, loss_pattern):
    """
    Print a detailed factor table comparing exposure vector to QE factors.
    
    Args:
        exposure_vector: List of exposure entries
        loss_pattern: Pattern object with QE factors
    """
    
    print(f"\n{'Index':<6} {'Quarter End':<12} {'Exposure':<12} {'QE Factor':<12} {'Duration':<8} {'Type':<6} {'Match':<6}")
    print(f"{'-'*70}")
    
    total_exposure = 0.0
    total_qe = 0.0
    perfect_mapping = True
    
    for i, (exp_entry, qe_element) in enumerate(zip(exposure_vector, loss_pattern.elements)):
        exp_date = exp_entry.date_bucket
        exp_value = exp_entry.sum
        qe_date = getattr(qe_element, 'exposure_date', None)
        qe_value = qe_element.distribution
        qe_duration = getattr(qe_element, 'duration', 0)
        qe_type = getattr(qe_element, 'period_frequency', 'QE')
        
        # Check if values match
        values_match = abs(exp_value - qe_value) < 0.000001
        dates_match = exp_date == qe_date
        perfect_match = values_match and dates_match
        
        if not perfect_match:
            perfect_mapping = False
        
        match_status = "✅" if perfect_match else "❌"
        
        # Format the date properly
        date_str = str(exp_date) if exp_date else 'N/A'
        
        print(f"{i+1:<6} {date_str:<12} {exp_value:<12.6f} {qe_value:<12.6f} {qe_duration:<8} {qe_type:<6} {match_status:<6}")
        
        total_exposure += exp_value
        total_qe += qe_value
    
    print(f"{'-'*70}")
    print(f"{'TOTAL':<6} {'':<12} {total_exposure:<12.6f} {total_qe:<12.6f} {'':<8} {'':<6} {'✅' if perfect_mapping else '❌':<6}")
    
    if len(exposure_vector) != len(loss_pattern.elements):
        print(f"\n⚠️  WARNING: Count mismatch - Exposure: {len(exposure_vector)}, QE Factors: {len(loss_pattern.elements)}")

def main():
    """
    Main function to handle command line arguments and execute the calculation.
    """
    
    if len(sys.argv) != 2:
        print("Usage: python calculate_writing_loss_pattern.py <coverage_id>")
        print("Example: python calculate_writing_loss_pattern.py 100")
        sys.exit(1)
    
    try:
        coverage_id = int(sys.argv[1])
    except ValueError:
        print("Error: Coverage ID must be an integer")
        sys.exit(1)
    
    print_factor_table(coverage_id)

if __name__ == "__main__":
    main()
