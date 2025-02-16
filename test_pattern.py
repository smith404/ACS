# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from interpolation_utils import spline_interpolation
from pattern import Pattern
from pattern_evaluator import PatternEvaluator
from pattern_slice import PatternSlice
from utils import cumulative_sum, scale_vector_to_sum


def main():
    pattern = Pattern(360)
    pattern.set_identifier("Test Pattern")
    pattern.add_slice(PatternSlice(0, 0.1284))
    pattern.add_slice(PatternSlice())
    pattern.add_slice(PatternSlice())
    pattern.add_slice(PatternSlice())

    pattern.distribute_remaining()
    pattern.align_slice_periods()

    pattern.display()
    print("Distribution check:", pattern.check_distribution())
    print("Duration check:", pattern.check_durations())

    pattern.save_to_file("scratch/my_test_pattern.json")

    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    for block in evaluator.patternBlocks:
        print(block)
        
    evaluator.apply_ultimate_value(120)
    print("Sum of values by points:", evaluator.sum_ultimate_values_by_points())
    print("Sum of cumulative values by points:", evaluator.cumulative_sum_by_points(evaluator.sum_ultimate_values_by_points()))

    print(f"Interpolatd value (210) = {spline_interpolation(evaluator.cumulative_sum_by_points(evaluator.sum_ultimate_values_by_points()), 210)}")
 
    print(f"LIC: {evaluator.sum_ultimate_values(evaluator.evaluate_lic_blocks(1))}")
    print(f"LRC: {evaluator.sum_ultimate_values(evaluator.evaluate_lrc_blocks(1))}")
    print(f"UPR: {evaluator.sum_ultimate_values(evaluator.evaluate_upr_blocks(1))}")
    print(f"Written: {evaluator.sum_ultimate_values(evaluator.evaluate_written_blocks(1))}")
    print(f"Unwritten: {evaluator.sum_ultimate_values(evaluator.evaluate_unwritten_blocks(1))}")
    print(f"Total: {evaluator.sum_ultimate_values(evaluator.patternBlocks)}")

    largestHeightPerDisplayLevel = evaluator.find_largest_height_per_display_level(evaluator.patternBlocks)
    print(f"Largest heights per display level: {largestHeightPerDisplayLevel}")

    minPoint, maxPoint = PatternEvaluator.find_min_max_points(evaluator.patternBlocks)
    
    print(cumulative_sum(scale_vector_to_sum(largestHeightPerDisplayLevel, maxPoint - minPoint))) 


if __name__ == "__main__":
    main()
