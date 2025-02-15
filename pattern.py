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
from typing import List, Optional, Tuple

from utils import cumulative_sum, save_string_to_file, scale_vector_to_sum

class Pattern:
    def __init__(self, duration: int = 360):
        self.identifier = uuid.uuid4()
        self.slices: List[PatternSlice] = []
        self.duration = duration

    def add_slice(self, pattern_slice: 'PatternSlice'):
        if isinstance(pattern_slice, PatternSlice):
            pattern_slice.set_duration(self.duration)
            self.slices.append(pattern_slice)
        else:
            raise TypeError("Expected a PatternSlice instance")

    def set_identifier(self, identifier: str):
        self.identifier = identifier

    def set_duration(self, duration: int):
        self.duration = duration
        for slice in self.slices:
            slice.set_duration(duration)

    def align_slice_periods(self, development_periods: Optional[int] = None):
        if development_periods is None or development_periods == 0:
            development_periods = len(self.slices)
        for index, slice in enumerate(self.slices):
            slice.set_development_periods(development_periods)
            slice.set_duration_offset(self.duration / development_periods)
            slice.set_start_offset(index * (self.duration / development_periods))

    def delete_slice(self, pattern_slice: Optional['PatternSlice'] = None):
        if pattern_slice in self.slices:
            self.slices.remove(pattern_slice)
        else:
            raise ValueError("PatternSlice not found in slices")

    def expand(self):
        for slice in self.slices:
            if slice.start_distribution != 0:
                slice.iterate_start_periods()
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

    def check_distribution(self) -> bool:
        total_distribution = sum(slice.distribution + slice.start_distribution for slice in self.slices)
        return total_distribution == 1

    def check_durations(self) -> bool:
        return all(slice.duration == self.duration for slice in self.slices)

    def get_all_pattern_blocks(self) -> List['PatternBlock']:
        blocks = []
        display_level = 0
        for index, slice in enumerate(self.slices):
            blocks.extend(slice.get_pattern_blocks(pattern_id=self.identifier, slice_number=index, display_level=display_level))
            display_level += 2 if slice.start_distribution != 0 else 1
        return blocks

    def display(self):
        print(self)
        for slice in self.slices:
            print(slice)

    def save_to_file(self, filename: str):
        with open(filename, 'w') as file:
            json.dump({
                'identifier': str(self.identifier),
                'duration': self.duration,
                'slices': [slice.__dict__ for slice in self.slices]
            }, file)

    def to_json(self) -> str:
        return json.dumps({
            'identifier': str(self.identifier),
            'duration': self.duration,
            'slices': [slice.__dict__ for slice in self.slices]
        })

    @classmethod
    def load_from_file(cls, filename: str) -> 'Pattern':
        with open(filename, 'r') as file:
            data = json.load(file)
            pattern = cls(duration=data['duration'])
            pattern.identifier = data['identifier']
            pattern.slices = [PatternSlice(**slice_data) for slice_data in data['slices']]
            return pattern

    def __str__(self) -> str:
        return f"Pattern with {len(self.slices)} slices and duration {self.duration}"

class PatternSlice:
    def __init__(self, distribution: float = 0, start_distribution: float = 0, duration: int = 0, start_offset: int = 0, duration_offset: int = 0, development_periods: int = 0):
        self.distribution = distribution
        self.start_distribution = start_distribution
        self.duration = duration
        self.start_offset = start_offset
        self.duration_offset = duration_offset
        self.development_periods = development_periods

    def set_duration(self, duration: int):
        self.duration = duration

    def set_start_offset(self, start_offset: int):
        self.start_offset = start_offset

    def set_duration_offset(self, duration_offset: int):
        self.duration_offset = duration_offset

    def set_development_periods(self, development_periods: int):
        self.development_periods = development_periods

    def iterate_development_periods(self):
        for index in range(self.development_periods + 1):
            factor = self.development_periods * 2 if index == 0 or index == self.development_periods else self.development_periods
            print(f"({self.start_offset + (index * self.duration_offset)} , {(self.start_offset + ((index + 1) * self.duration_offset)) - 1} , {self.distribution / factor})", end='\t')
        print()

    def iterate_start_periods(self):
        for index in range(self.development_periods):
            print(f"({self.start_offset + (index * self.duration_offset)} , {(self.start_offset + ((index + 1) * self.duration_offset)) - 1} , {self.start_distribution / self.development_periods})", end='\t')
        print()

    def get_pattern_blocks(self, pattern_id: str, slice_number: int = 0, display_level: int = 0) -> List['PatternBlock']:
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

    def to_json(self) -> str:
        return json.dumps({
            'distribution': self.distribution,
            'start_distribution': self.start_distribution,
            'duration': self.duration,
            'start_offset': self.start_offset,
            'duration_offset': self.duration_offset,
            'development_periods': self.development_periods
        })

    def __str__(self) -> str:
        return f"PatternSlice: (Distribution: {self.distribution}, Start Distribution: {self.start_distribution}, Duration: {self.duration}, Start Offset: {self.start_offset}, Duration Offset: {self.duration_offset}, Development Periods: {self.development_periods})"

class BlockShape(str, Enum):
    LTRIANGLE = "LTRIANGLE"
    RTRIANGLE = "RTRIANGLE"
    RECTANGLE = "RECTANGLE"

class PatternBlock:
    def __init__(self, pattern: str, slice_number: int = 0, display_level: int = 0, start_point: int = 0, end_point: int = 0, height: float = 0, shape: BlockShape = BlockShape.RECTANGLE):
        self.pattern = pattern
        self.slice_number = slice_number
        self.display_level = display_level
        self.start_point = start_point
        self.end_point = end_point
        self.height = height
        self.ultimate_value = height
        self.shape = shape

    def generate_polygon(self, colour: str = "blue", stroke: str = "black", y_axis: int = 0, height: int = 50) -> str:
        points = self._generate_points(y_axis, height)
        return f'<polygon vector-effect="non-scaling-stroke" stroke-width="1" points="{points}" fill="{colour}" stroke="{stroke}" />'

    def _generate_points(self, y_axis: int, height: int) -> str:
        if self.shape == BlockShape.RECTANGLE:
            return f"{self.start_point},{y_axis} {self.end_point + 1},{y_axis} {self.end_point + 1},{y_axis + height} {self.start_point},{y_axis + height}"
        elif self.shape == BlockShape.LTRIANGLE:
            return f"{self.start_point},{y_axis} {self.end_point + 1},{y_axis} {self.end_point + 1},{y_axis + height}"
        elif self.shape == BlockShape.RTRIANGLE:
            return f"{self.start_point},{y_axis} {self.start_point},{y_axis + height} {self.end_point + 1},{y_axis + height}"
        return ""

    def __str__(self) -> str:
        return f"PatternBlock with Pattern: {self.pattern}, Slice Number: {self.slice_number}, Display Level: {self.display_level}, Start Point: {self.start_point}, End Point: {self.end_point}, Height: {self.height}, Ultimate Value: {self.ultimate_value}, Shape: {self.shape.name}"

class PatternEvaluator:
    def __init__(self, pattern_blocks: List[PatternBlock]):
        if not all(isinstance(block, PatternBlock) for block in pattern_blocks):
            raise TypeError("All elements must be instances of PatternBlock")
        self.pattern_blocks = pattern_blocks

    def apply_ultimate_value(self, ultimate_value: float):
        for block in self.pattern_blocks:
            block.ultimate_value = block.height * ultimate_value

    def get_pattern_blocks_less_than(self, point: int) -> List[PatternBlock]:
        return [block for block in self.pattern_blocks if block.start_point < point]

    def get_pattern_blocks_greater_than(self, point: int) -> List[PatternBlock]:
        return [block for block in self.pattern_blocks if block.start_point > point]

    def get_pattern_blocks_between(self, start_point: int, end_point: int) -> List[PatternBlock]:
        return [block for block in self.pattern_blocks if start_point <= block.start_point < end_point]

    def evaluate_written_blocks(self, latest_written_slice: int) -> List[PatternBlock]:
        return [block for block in self.pattern_blocks if block.slice_number <= latest_written_slice]

    def evaluate_unwritten_blocks(self, latest_written_slice: int) -> List[PatternBlock]:
        return [block for block in self.pattern_blocks if block.slice_number > latest_written_slice]

    def evaluate_lic_blocks(self, latest_written_slice: int) -> List[PatternBlock]:
        end_point_of_latest_slice = self.get_earliest_end_point_of_slice(latest_written_slice)
        return [block for block in self.pattern_blocks if block.slice_number <= latest_written_slice and block.end_point <= end_point_of_latest_slice]

    def evaluate_lrc_blocks(self, latest_written_slice: int) -> List[PatternBlock]:
        end_point_of_latest_slice = self.get_earliest_end_point_of_slice(latest_written_slice)
        return [block for block in self.pattern_blocks if block.slice_number <= latest_written_slice and block.end_point > end_point_of_latest_slice]

    def evaluate_upr_blocks(self, latest_written_slice: int) -> List[PatternBlock]:
        end_point_of_latest_slice = self.get_earliest_end_point_of_slice(latest_written_slice)
        return [block for block in self.pattern_blocks if block.end_point > end_point_of_latest_slice]

    def get_earliest_end_point_of_slice(self, slice_number: int) -> Optional[int]:
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        return min((block.end_point for block in slice_blocks), default=None)

    def get_earliest_start_point_of_slice(self, slice_number: int) -> Optional[int]:
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        return min((block.start_point for block in slice_blocks), default=None)

    def get_latest_end_point_of_slice(self, slice_number: int) -> Optional[int]:
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        return max((block.end_point for block in slice_blocks), default=None)

    def get_latest_start_point_of_slice(self, slice_number: int) -> Optional[int]:
        slice_blocks = [block for block in self.pattern_blocks if block.slice_number == slice_number]
        return max((block.start_point for block in slice_blocks), default=None)

    def save_to_file(self, filename: str):
        with open(filename, 'w') as file:
            json.dump([block.__dict__ for block in self.pattern_blocks], file)

    @classmethod
    def load_from_file(cls, filename: str) -> 'PatternEvaluator':
        with open(filename, 'r') as file:
            blocks_data = json.load(file)
            pattern_blocks = [PatternBlock(**block_data) for block_data in blocks_data]
            return cls(pattern_blocks)

    @staticmethod
    def create_svg(pattern_blocks: List[PatternBlock], latest_written_slice: Optional[int] = None, day_cut: Optional[int] = None, slice_heights: dict[int, int] = None, height_scale: float = 0.7, pre_colour: str = "white", colour: str = "blue", condition: str = "or") -> str:
        min_point, max_point = PatternEvaluator.find_min_max_points(pattern_blocks)
        width = max_point - min_point
        if slice_heights is None:
            largest_height_per_display_level = PatternEvaluator.find_largest_height_per_display_level(pattern_blocks)
            slice_heights = cumulative_sum(scale_vector_to_sum(largest_height_per_display_level, (max_point - min_point)*height_scale))
        height, svg_elements = PatternEvaluator.generate_svg_elements(pattern_blocks, latest_written_slice, day_cut, slice_heights, pre_colour, colour, condition)
        return f'<svg width="100%" height="auto" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">{"".join(svg_elements)}</svg>'

    @staticmethod
    def generate_svg_elements(pattern_blocks: List[PatternBlock], latest_written_slice: Optional[int], day_cut: Optional[int], slice_heights: dict[int, int], pre_colour: str, colour: str, condition: str) -> Tuple[int, List[str]]:
        svg_elements = []
        slice_height = 0
        y_axis = 0
        height = 0
        for block in pattern_blocks:
            block_colour = PatternEvaluator.determine_block_colour(block, latest_written_slice, day_cut, pre_colour, colour, condition)
            if block.display_level == 0:
                slice_height = slice_heights[block.display_level]
                y_axis = 0
                if slice_heights[block.display_level] > height:
                    height = slice_heights[block.display_level]
            else:
                slice_height = slice_heights[block.display_level]-slice_heights[block.display_level-1]
                y_axis = slice_heights[block.display_level-1]
                if slice_heights[block.display_level] > height:
                    height = slice_heights[block.display_level]
            element = block.generate_polygon(block_colour, y_axis=y_axis, height=slice_height)
            svg_elements.append(element)
        return height, svg_elements

    @staticmethod
    def determine_block_colour(block: PatternBlock, latest_written_slice: Optional[int], day_cut: Optional[int], pre_colour: str, colour: str, condition: str) -> str:
        block_colour = colour
        if condition == "and":
            if (latest_written_slice is not None and block.slice_number <= latest_written_slice) and (day_cut is not None and block.start_point <= day_cut):
                block_colour = pre_colour
        elif condition == "or":
            if (latest_written_slice is not None and block.slice_number <= latest_written_slice) or (day_cut is not None and block.start_point <= day_cut):
                block_colour = pre_colour
        return block_colour

    @staticmethod
    def find_min_max_points(pattern_blocks: List[PatternBlock]) -> Tuple[Optional[int], Optional[int]]:
        if not pattern_blocks:
            return None, None
        min_start_point = min(block.start_point for block in pattern_blocks)
        max_end_point = max(block.end_point for block in pattern_blocks)
        return min_start_point, max_end_point

    @staticmethod
    def sum_ultimate_values(pattern_blocks: List[PatternBlock]) -> float:
        return sum(block.ultimate_value for block in pattern_blocks)

    @staticmethod
    def sum_block_heights(pattern_blocks: List['PatternBlock']) -> float:
        return sum(block.height for block in pattern_blocks)

    @staticmethod
    def find_largest_height_per_display_level(pattern_blocks: List[PatternBlock]) -> dict:
        largest_heights = {}
        for block in pattern_blocks:
            if block.display_level not in largest_heights or block.height > largest_heights[block.display_level]:
                largest_heights[block.display_level] = block.height
        return largest_heights

    def __str__(self) -> str:
        return f"PatternEvaluator with {len(self.pattern_blocks)} blocks"

def main():
    pattern = Pattern(360)
    pattern.set_identifier("Test Pattern")
    pattern.add_slice(PatternSlice(0, 0.05))
    pattern.add_slice(PatternSlice())
    pattern.add_slice(PatternSlice(0, 0.05))
    pattern.add_slice(PatternSlice())

    pattern.distribute_remaining()
    pattern.align_slice_periods()

    pattern.display()
    print("Distribution check:", pattern.check_distribution())
    print("Duration check:", pattern.check_durations())

    pattern.save_to_file("scratch/my_test_pattern.json")

    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    for block in evaluator.pattern_blocks:
        print(block)
        
    evaluator.apply_ultimate_value(136.5)

    print(f"LIC: {evaluator.sum_ultimate_values(evaluator.evaluate_lic_blocks(1))}")
    print(f"LRC: {evaluator.sum_ultimate_values(evaluator.evaluate_lrc_blocks(1))}")
    print(f"UPR: {evaluator.sum_ultimate_values(evaluator.evaluate_upr_blocks(1))}")
    print(f"Written: {evaluator.sum_ultimate_values(evaluator.evaluate_written_blocks(1))}")
    print(f"Unwritten: {evaluator.sum_ultimate_values(evaluator.evaluate_unwritten_blocks(1))}")
    print(f"Total: {evaluator.sum_ultimate_values(evaluator.pattern_blocks)}")

    largest_height_per_display_level = evaluator.find_largest_height_per_display_level(evaluator.pattern_blocks)
    print(f"Largest heights per display level: {largest_height_per_display_level}")
    min_point, max_point = PatternEvaluator.find_min_max_points(evaluator.pattern_blocks)
    print(cumulative_sum(scale_vector_to_sum(largest_height_per_display_level, max_point - min_point))) 

if __name__ == "__main__":
    main()
