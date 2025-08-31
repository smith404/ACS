"""
Test script to validate duration < 1 error detection
"""

from coverage_pattern_reader import create_coverage_pattern_reader
import os
import shutil

def create_test_data_with_invalid_duration():
    """Create test CSV data with duration < 1 to test validation."""
    
    # Create a temporary test directory
    test_dir = "scratch/test_duration"
    os.makedirs(test_dir, exist_ok=True)
    
    # Copy existing valid data first
    shutil.copy("scratch/gold/host_coverage_pattern_allocation.csv", test_dir)
    shutil.copy("scratch/gold/host_pattern.csv", test_dir)
    
    # Create modified pattern factors with some duration < 1 (must be integers)
    pattern_factors_content = """pattern_id,period,period_frequency,factor_value,duration
1000,1,Q,1.0,0
1001,1,QS,0.128372,0
1001,1,Q,0.0,1
1001,2,Q,0.212891,2
1001,3,Q,0.346369,3
1001,4,Q,0.312368,4
1002,1,QS,1.0,0
"""
    
    with open(f"{test_dir}/host_pattern_factor.csv", "w") as f:
        f.write(pattern_factors_content)
    
    print(f"✅ Created test data in {test_dir}")
    print("   - Pattern 1000: Duration 0 (< 1) ❌")
    print("   - Pattern 1001: QS Duration 0 (< 1) ❌") 
    print("   - Pattern 1002: Duration 0 (< 1) ❌")
    
    return test_dir

def test_duration_validation():
    """Test that duration < 1 validation works correctly."""
    
    print("=== Testing Duration < 1 Validation ===\n")
    
    # Create test data with invalid durations
    test_dir = create_test_data_with_invalid_duration()
    
    try:
        # Test with the invalid data
        reader = create_coverage_pattern_reader(data_path=test_dir)
        
        print(f"\n{'='*60}")
        print("Testing Coverage 100 (Pattern 1000 - Duration 0)")
        print('='*60)
        
        pattern_summary = reader.get_pattern_summary(100)
        if 'error' not in pattern_summary:
            print(f"Pattern loaded: {pattern_summary['pattern_name']}")
        
        print(f"\n{'='*60}")
        print("Testing Coverage 101 (Pattern 1001 - QS Duration 0)")
        print('='*60)
        
        pattern_summary = reader.get_pattern_summary(101)
        if 'error' not in pattern_summary:
            print(f"Pattern loaded: {pattern_summary['pattern_name']}")
        
        print(f"\n{'='*60}")
        print("Testing Coverage 102 (Pattern 1002 - Duration 0)")
        print('='*60)
        
        pattern_summary = reader.get_pattern_summary(102)
        if 'error' not in pattern_summary:
            print(f"Pattern loaded: {pattern_summary['pattern_name']}")
    
    finally:
        # Clean up test directory
        shutil.rmtree(test_dir)
        print(f"\n✅ Cleaned up test directory: {test_dir}")

def test_valid_data_no_errors():
    """Test that valid data (all durations >= 1) produces no errors."""
    
    print(f"\n{'='*60}")
    print("Testing Valid Data (No Duration Errors Expected)")
    print('='*60)
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Test all coverages with valid data
    for coverage_id in [100, 101, 102]:
        print(f"\nTesting Coverage {coverage_id}:")
        pattern_summary = reader.get_pattern_summary(coverage_id)
        if 'error' not in pattern_summary:
            print(f"  ✅ Pattern: {pattern_summary['pattern_name']} (No duration errors)")
        else:
            print(f"  ❌ Error: {pattern_summary['error']}")

if __name__ == "__main__":
    test_duration_validation()
    test_valid_data_no_errors()
