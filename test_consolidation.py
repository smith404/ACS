"""
Test script to demonstrate QS/Q factor consolidation
"""

from coverage_pattern_reader import create_coverage_pattern_reader

def test_factor_consolidation():
    """Test that QS and Q factors for the same period are properly consolidated."""
    
    print("=== Factor Consolidation Test ===")
    
    reader = create_coverage_pattern_reader(data_path="scratch/gold")
    
    # Test pattern 1001 which has both QS and Q for period 1
    coverage_id = 101
    pattern = reader.construct_pattern_object(coverage_id)
    
    print(f"\nPattern: {pattern.name} (ID: {getattr(pattern, 'pattern_id', 'N/A')})")
    print(f"Total PatternFactor elements: {len(pattern.elements)}")
    
    # Show detailed breakdown of each PatternFactor element
    for i, element in enumerate(pattern.elements, 1):
        period = getattr(element, 'period', f'Element_{i}')
        up_front = element.up_front
        distribution = element.distribution
        up_front_duration = getattr(element, 'up_front_duration', 'N/A')
        distribution_duration = getattr(element, 'distribution_duration', 'N/A')
        
        print(f"\nPatternFactor {i} (Period {period}):")
        print(f"  - Upfront Value: {up_front:.4f} (Duration: {up_front_duration})")
        print(f"  - Distribution Value: {distribution:.4f} (Duration: {distribution_duration})")
        print(f"  - Type: {element.type.name if hasattr(element, 'type') else 'N/A'}")
        
        # Show if this element has both upfront and distributed (consolidated)
        if up_front > 0 and distribution > 0:
            print(f"  *** CONSOLIDATED: Period {period} has both QS (upfront) and Q (distributed) ***")
        elif up_front > 0:
            print(f"  - QS only (upfront)")
        elif distribution > 0:
            print(f"  - Q only (distributed)")
    
    # Verify totals
    total_upfront = sum(e.up_front for e in pattern.elements)
    total_distributed = sum(e.distribution for e in pattern.elements)
    
    print(f"\nTotals:")
    print(f"  - Total Upfront: {total_upfront:.4f}")
    print(f"  - Total Distributed: {total_distributed:.4f}")
    print(f"  - Grand Total: {total_upfront + total_distributed:.4f}")
    print(f"  - Distribution Valid: {pattern.check_distribution()}")

if __name__ == "__main__":
    test_factor_consolidation()
