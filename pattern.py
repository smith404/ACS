# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from enum import Enum
import json
import uuid

from utils import store_string_to_file

class Pattern:
    def __init__(self, duration=0):
        self.identifier = uuid.uuid4()
        self.slices = []
        self.duration = duration

    def add_slice(self, pattern_slice):
        if isinstance(pattern_slice, PatternSlice):
            pattern_slice.set_duration(self.duration)
            self.slices.append(pattern_slice)
        else:
            raise TypeError("Expected a PatternSlice instance")

    def set_identifier(self, identifier):
        self.identifier = identifier

    def set_duration(self, duration):
        self.duration = duration
        for slice in self.slices:
            slice.set_duration(duration)

    def align_slice_periods(self, development_periods=None):
        if development_periods is None or development_periods == 0:
            development_periods = len(self.slices)
        for index, slice in enumerate(self.slices, start=0):
            slice.set_development_periods(development_periods)
            slice.set_duration_offset(self.duration / development_periods)
            slice.set_start_offset(index * (self.duration / development_periods))

    def delete_slice(self, pattern_slice=None):
        if pattern_slice in self.slices:
            self.slices.remove(pattern_slice)
        else:
            raise ValueError("PatternSlice not found in slices")

    def expand(self):
        for index, slice in enumerate(self.slices, start=1):
            # Check for a start distribution
            if slice.start_distribution != 0:
                slice.iterate_start_periods()
            # Check for a distribution
            if slice.distribution != 0:
                slice.iterate_development_periods()

    def distribute_remaining(self):
        total_distribution = sum(slice.distribution + slice.start_distribution for slice in self.slices)
        if total_distribution < 1:
            remaining = 1 - total_distribution
            for slice in self.slices:
                slice.distribution += remaining / len(self.slices)

    def set_all_distributions_to_zero(self):
        for slice in self.slices:
            slice.distribution = 0

    def set_all_start_distributions_to_zero(self):
        for slice in self.slices:
            slice.start_distribution = 0

    def set_pattern_to_zero(self):
        self.set_all_start_distributions_to_zero()
        self.set_all_distributions_to_zero()

    def check_distribution(self):
        total_distribution = sum(slice.distribution + slice.start_distribution for slice in self.slices)
        return total_distribution == 1

    def check_durations(self):
        return all(slice.duration == self.duration for slice in self.slices)

    def get_all_pattern_blocks(self):
        blocks = []
        display_level = 0
        for index, slice in enumerate(self.slices, start=0):
            blocks.extend(slice.get_pattern_blocks(pattern_id=self.identifier, slice_number=index, display_level=display_level))
            if slice.start_distribution != 0:
                display_level = display_level + 2
            else:
                display_level = display_level + 1
        return blocks

    def display(self):
        print(self)
        for slice in self.slices:
            print(slice)

    def save_to_file(self, filename):
        with open(filename, 'w') as file:
            json.dump({
                'identifier': str(self.identifier),
                'duration': self.duration,
                'slices': [slice.__dict__ for slice in self.slices]
            }, file)

    @classmethod
    def load_from_file(cls, filename):
        with open(filename, 'r') as file:
            data = json.load(file)
            pattern = cls(duration=data['duration'])
            pattern.identifier = data['identifier']
            pattern.slices = [PatternSlice(**slice_data) for slice_data in data['slices']]
            return pattern

    def __str__(self):
        return f"Pattern with {len(self.slices)} slices and duration {self.duration}"

class PatternSlice:
    def __init__(self, distribution=0, start_distribution=0, duration=0, start_offset=0, duration_offset=0, development_periods=0):
        self.distribution = distribution
        self.start_distribution = start_distribution
        self.duration = duration
        self.start_offset = start_offset
        self.duration_offset = duration_offset
        self.development_periods = development_periods

    def set_duration(self, duration):
        self.duration = duration

    def set_start_offset(self, start_offset):
        self.start_offset = start_offset

    def set_duration_offset(self, duration_offset):
        self.duration_offset = duration_offset

    def set_development_periods(self, development_periods):
        self.development_periods = development_periods

    def iterate_development_periods(self):
        for index in range(0, self.development_periods + 1):
            factor = self.development_periods
            # Check for the first and last index as these are half the value of the other indexes
            if index == 0 or index == self.development_periods:
                factor = factor * 2
            print(f"({self.start_offset+(index * self.duration_offset)} , {(self.start_offset+((index + 1) * self.duration_offset)) - 1} , {self.distribution/factor})", end='\t')
        # For a new line after the loop
        print()

    def iterate_start_periods(self):
        factor = self.development_periods
        for index in range(self.development_periods):
            print(f"({self.start_offset+(index * self.duration_offset)} , {(self.start_offset+((index + 1) * self.duration_offset)) - 1} , {self.start_distribution/factor})", end='\t')
        # For a new line after the loop
        print()

    def get_pattern_blocks(self, pattern_id, slice_number=0, display_level=0):
        blocks = []
        if self.start_distribution != 0:
            for index in range(self.development_periods):
                shape = BlockShape.RECTANGLE
                start_point = self.start_offset + (index * self.duration_offset)
                end_point = self.start_offset + ((index + 1) * self.duration_offset) - 1
                block = PatternBlock(pattern_id, slice_number=slice_number, display_level=display_level, start_point=start_point, end_point=end_point, height=self.start_distribution / self.development_periods, shape=shape)
                blocks.append(block)
            display_level = display_level + 1
        if self.distribution != 0:
            for index in range(0, self.development_periods + 1):
                shape = BlockShape.RECTANGLE
                factor = self.development_periods
                # Check for the first and last index as these are half the value of the other indexes
                if index == 0 or index == self.development_periods:
                    factor = factor * 2
                    if index == 0:
                        shape = BlockShape.LTRIANGLE
                    else:
                        shape = BlockShape.RTRIANGLE
                start_point = self.start_offset+(index * self.duration_offset)
                end_point = (self.start_offset+((index + 1) * self.duration_offset)) - 1
                block = PatternBlock(pattern_id, slice_number=slice_number, display_level=display_level, start_point=start_point, end_point=end_point, height=self.distribution / factor, shape=shape)
                blocks.append(block)
        return sorted(blocks, key=lambda block: block.start_point)

    def __str__(self):
        return f"PatternSlice: (Distribution: {self.distribution}, Start Distribution: {self.start_distribution}, Duration: {self.duration}, Start Offset: {self.start_offset}, Duration Offset: {self.duration_offset}, Development Periods: {self.development_periods})"

class BlockShape(str, Enum):
    LTRIANGLE = "LTRIANGLE"
    RTRIANGLE = "RTRIANGLE"
    RECTANGLE = "RECTANGLE"

class PatternBlock:
    def __init__(self, pattern, slice_number=0, display_level=0, start_point=0, end_point=0, height=0, shape=BlockShape.RECTANGLE):
        self.pattern = pattern
        self.slice_number = slice_number
        self.display_level = display_level
        self.start_point = start_point
        self.end_point = end_point
        self.height = height
        self.ultimate_value = height
        self.shape = shape

    def generate_polygon(self, colour="blue", stroke="black", y_axis=0, height=50):
        if self.shape == BlockShape.RECTANGLE:
            points = f"{self.start_point},{y_axis} {self.end_point + 1},{y_axis} {self.end_point + 1},{y_axis + height} {self.start_point},{y_axis + height}"
        elif self.shape == BlockShape.LTRIANGLE:
            points = f"{self.start_point},{y_axis} {self.end_point + 1},{y_axis} {self.end_point + 1},{y_axis + height}"
        elif self.shape == BlockShape.RTRIANGLE:
            points = f"{self.start_point},{y_axis} {self.start_point},{y_axis + height} {self.end_point + 1},{y_axis + height} "
        return f'<polygon vector-effect="non-scaling-stroke" stroke-width="1" points="{points}" fill="{colour}" stroke="{stroke}" />'

    def __str__(self):
        return f"PatternBlock with Pattern: {self.pattern}, Slice Number: {self.slice_number}, Display Level: {self.display_level}, Start Point: {self.start_point}, End Point: {self.end_point}, Height: {self.height}, Ultimate Value: {self.ultimate_value}, Shape: {self.shape.name}"

class PatternEvaluator:
    def __init__(self, pattern_blocks):
        if not all(isinstance(block, PatternBlock) for block in pattern_blocks):
            raise TypeError("All elements must be instances of PatternBlock")
        self.pattern_blocks = pattern_blocks

    def apply_ultimate_value(self, ultimate_value):
       for block in self.pattern_blocks:
            block.ultimate_value = block.height * ultimate_value

    def get_pattern_blocks_less_than(self, point):
        return [block for block in self.pattern_blocks if block.start_point < point]

    def get_pattern_blocks_greater_than(self, point):
        return [block for block in self.pattern_blocks if block.start_point > point]

    def get_pattern_blocks_between(self, start_point, end_point):
        return [block for block in self.pattern_blocks if start_point <= block.start_point < end_point]

    def evaluate_written_blocks(self, latest_written_slice):
        if not self.pattern_blocks:
            return []
        return [block for block in self.pattern_blocks if block.slice_number <= latest_written_slice]

    def evaluate_unwritten_blocks(self, latest_written_slice):
        if not self.pattern_blocks:
            return []
        return [block for block in self.pattern_blocks if block.slice_number > latest_written_slice]

    def evaluate_lic_blocks(self, latest_written_slice):
        if not self.pattern_blocks:
            return []
        end_point_of_latest_slice = min(block.end_point for block in self.pattern_blocks if block.slice_number == latest_written_slice)
        return [block for block in self.pattern_blocks if block.slice_number <= latest_written_slice and block.end_point <= end_point_of_latest_slice]

    def evaluate_lrc_blocks(self, latest_written_slice):
        if not self.pattern_blocks:
            return []
        end_point_of_latest_slice = min(block.end_point for block in self.pattern_blocks if block.slice_number == latest_written_slice)
        return [block for block in self.pattern_blocks if block.slice_number <= latest_written_slice and block.end_point > end_point_of_latest_slice]

    def evaluate_upr_blocks(self, latest_written_slice):
        if not self.pattern_blocks:
            return []
        end_point_of_latest_slice = min(block.end_point for block in self.pattern_blocks if block.slice_number == latest_written_slice)
        return [block for block in self.pattern_blocks if block.end_point > end_point_of_latest_slice]

    def get_earliest_end_point_of_slice(self, slice_number):
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        if not slice_blocks:
            return None
        return min(block.end_point for block in slice_blocks)

    def get_earliest_start_point_of_slice(self, slice_number):
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        if not slice_blocks:
            return None
        return min(block.start_point for block in slice_blocks)

    def get_latest_end_point_of_slice(self, slice_number):
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        if not slice_blocks:
            return None
        return max(block.end_point for block in slice_blocks)

    def get_latest_start_point_of_slice(self, slice_number):
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        if not slice_blocks:
            return None
        return max(block.start_point for block in slice_blocks)

    def save_to_file(self, filename):
        with open(filename, 'w') as file:
            json.dump([block.__dict__ for block in self.pattern_blocks], file)

    @classmethod
    def load_from_file(cls, filename):
        with open(filename, 'r') as file:
            blocks_data = json.load(file)
            pattern_blocks = [PatternBlock(**block_data) for block_data in blocks_data]
            return cls(pattern_blocks)

    @staticmethod
    def create_svg(pattern_blocks, latest_written_slice=None, day_cut=None, slice_height=50, pre_colour="white", colour="blue"):
        min_point, max_point = PatternEvaluator.find_min_max_points(pattern_blocks)
        width = max_point - min_point
        height = 0
        svg_elements = []
        for block in pattern_blocks:
            block_colour = colour
            if latest_written_slice is not None and block.slice_number <= latest_written_slice:
                block_colour = pre_colour
            if day_cut is not None and block.start_point <= day_cut:
                block_colour = pre_colour
            element = block.generate_polygon(block_colour, y_axis=slice_height * block.display_level, height=slice_height)
            svg_elements.append(element)
            if height < slice_height * block.display_level:
                height = slice_height * block.display_level
        return f'<svg width="100%" height="auto" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">{"".join(svg_elements)}</svg>'

    @staticmethod
    def find_min_max_points(pattern_blocks):
        if not pattern_blocks:
            return None, None
        min_start_point = min(block.start_point for block in pattern_blocks)
        max_end_point = max(block.end_point for block in pattern_blocks)
        return min_start_point, max_end_point

    @staticmethod
    def find_lowest_start_point_heights(pattern_blocks):
        if not pattern_blocks:
            return {}
        lowest_start_point_heights = {}
        for block in pattern_blocks:
            if block.display_level not in lowest_start_point_heights or block.start_point < lowest_start_point_heights[block.display_level]['start_point']:
                lowest_start_point_heights[block.display_level] = {
                    'start_point': block.start_point,
                    'height': block.height
                }
        return {level: data['height'] for level, data in lowest_start_point_heights.items()}

    @staticmethod
    def sum_ultimate_values(pattern_blocks):
        return sum(block.ultimate_value for block in pattern_blocks)

    def __str__(self):
        return f"PatternEvaluator with {len(self.pattern_blocks)} blocks"

    def __str__(self):
        return f"PatternEvaluator with {len(self.pattern_blocks)} blocks"

def main():
    pattern = Pattern(360)
    pattern.set_identifier("Test Pattern")
    pattern.add_slice(PatternSlice(0, 0.1))
    pattern.add_slice(PatternSlice())
    pattern.add_slice(PatternSlice(0, 0.1))
    pattern.add_slice(PatternSlice())

    pattern.distribute_remaining()
    pattern.align_slice_periods()

    pattern.display()
    print("Distribution check:", pattern.check_distribution())
    print("Duration check:", pattern.check_durations())

    pattern.save_to_file("scratch/my_test_pattern.json")

    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    evaluator.apply_ultimate_value(136.5)

    print(f"LIC: {evaluator.sum_ultimate_values(evaluator.evaluate_lic_blocks(1))}")
    print(f"LRC: {evaluator.sum_ultimate_values(evaluator.evaluate_lrc_blocks(1))}")
    print(f"UPR: {evaluator.sum_ultimate_values(evaluator.evaluate_upr_blocks(1))}")
    print(f"Written: {evaluator.sum_ultimate_values(evaluator.evaluate_written_blocks(1))}")
    print(f"Unwritten: {evaluator.sum_ultimate_values(evaluator.evaluate_unwritten_blocks(1))}")
    print(f"Total: {evaluator.sum_ultimate_values(evaluator.pattern_blocks)}")

    min_start, max_end = evaluator.find_min_max_points(evaluator.pattern_blocks)
    print(f"Min start point: {min_start}, Max end point: {max_end}")

    lowest_start_point_heights = evaluator.find_lowest_start_point_heights(evaluator.pattern_blocks)
    print(f"Lowest start point heights by display level: {lowest_start_point_heights}")

if __name__ == "__main__":
    main()
