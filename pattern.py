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

    def add_slice(self, patternSlice: 'PatternSlice'):
        if isinstance(patternSlice, PatternSlice):
            patternSlice.set_duration(self.duration)
            self.slices.append(patternSlice)
        else:
            raise TypeError("Expected a PatternSlice instance")

    def set_identifier(self, identifier: str):
        self.identifier = identifier

    def set_duration(self, duration: int):
        self.duration = duration
        for slice in self.slices:
            slice.set_duration(duration)

    def align_slice_periods(self, developmentPeriods: Optional[int] = None):
        if developmentPeriods is None or developmentPeriods == 0:
            developmentPeriods = len(self.slices)
        for index, slice in enumerate(self.slices):
            slice.set_development_periods(developmentPeriods)
            slice.set_duration_offset(self.duration / developmentPeriods)
            slice.set_start_offset(index * (self.duration / developmentPeriods))

    def delete_slice(self, patternSlice: Optional['PatternSlice'] = None):
        if patternSlice in self.slices:
            self.slices.remove(patternSlice)
        else:
            raise ValueError("PatternSlice not found in slices")

    def expand(self):
        for slice in self.slices:
            if slice.startDistribution != 0:
                slice.iterate_start_periods()
            if slice.distribution != 0:
                slice.iterate_development_periods()

    def distribute_remaining(self):
        totalDistribution = sum(slice.distribution + slice.startDistribution for slice in self.slices)
        if totalDistribution < 1:
            remaining = 1 - totalDistribution
            for slice in self.slices:
                slice.distribution += remaining / len(self.slices)

    def set_all_distributions_to_zero(self):
        for slice in self.slices:
            slice.distribution = 0

    def set_all_start_distributions_to_zero(self):
        for slice in self.slices:
            slice.startDistribution = 0

    def set_pattern_to_zero(self):
        self.set_all_start_distributions_to_zero()
        self.set_all_distributions_to_zero()

    def check_distribution(self) -> bool:
        totalDistribution = sum(slice.distribution + slice.startDistribution for slice in self.slices)
        return totalDistribution == 1

    def check_durations(self) -> bool:
        return all(slice.duration == self.duration for slice in self.slices)

    def get_all_pattern_blocks(self) -> List['PatternBlock']:
        blocks = []
        displayLevel = 0
        for index, slice in enumerate(self.slices):
            blocks.extend(slice.get_pattern_blocks(patternId=self.identifier, sliceNumber=index, displayLevel=displayLevel))
            displayLevel += 2 if slice.startDistribution != 0 else 1
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
            pattern.slices = [PatternSlice(**sliceData) for sliceData in data['slices']]
            return pattern

    def __str__(self) -> str:
        return f"Pattern with {len(self.slices)} slices and duration {self.duration}"

class PatternSlice:
    def __init__(self, distribution: float = 0, startDistribution: float = 0, duration: int = 0, startOffset: int = 0, durationOffset: int = 0, developmentPeriods: int = 0):
        self.distribution = distribution
        self.startDistribution = startDistribution
        self.duration = duration
        self.startOffset = startOffset
        self.durationOffset = durationOffset
        self.developmentPeriods = developmentPeriods

    def set_duration(self, duration: int):
        self.duration = duration

    def set_start_offset(self, startOffset: int):
        self.startOffset = startOffset

    def set_duration_offset(self, durationOffset: int):
        self.durationOffset = durationOffset

    def set_development_periods(self, developmentPeriods: int):
        self.developmentPeriods = developmentPeriods

    def iterate_development_periods(self):
        for index in range(self.developmentPeriods + 1):
            factor = self.developmentPeriods * 2 if index == 0 or index == self.developmentPeriods else self.developmentPeriods
            print(f"({self.startOffset + (index * self.durationOffset)} , {(self.startOffset + ((index + 1) * self.durationOffset)) - 1} , {self.distribution / factor})", end='\t')
        print()

    def iterate_start_periods(self):
        for index in range(self.developmentPeriods):
            print(f"({self.startOffset + (index * self.durationOffset)} , {(self.startOffset + ((index + 1) * self.durationOffset)) - 1} , {self.startDistribution / self.developmentPeriods})", end='\t')
        print()

    def get_pattern_blocks(self, patternId: str, sliceNumber: int = 0, displayLevel: int = 0) -> List['PatternBlock']:
        blocks = []
        if self.startDistribution != 0:
            for index in range(self.developmentPeriods):
                shape = BlockShape.RECTANGLE
                startPoint = self.startOffset + (index * self.durationOffset)
                endPoint = self.startOffset + ((index + 1) * self.durationOffset) - 1
                block = PatternBlock(patternId, sliceNumber=sliceNumber, displayLevel=displayLevel, startPoint=startPoint, endPoint=endPoint, height=self.startDistribution / self.developmentPeriods, shape=shape)
                blocks.append(block)
            displayLevel = displayLevel + 1
        if self.distribution != 0:
            for index in range(0, self.developmentPeriods + 1):
                shape = BlockShape.RECTANGLE
                factor = self.developmentPeriods
                if index == 0 or index == self.developmentPeriods:
                    factor = factor * 2
                    if index == 0:
                        shape = BlockShape.LTRIANGLE
                    else:
                        shape = BlockShape.RTRIANGLE
                startPoint = self.startOffset+(index * self.durationOffset)
                endPoint = (self.startOffset+((index + 1) * self.durationOffset)) - 1
                block = PatternBlock(patternId, sliceNumber=sliceNumber, displayLevel=displayLevel, startPoint=startPoint, endPoint=endPoint, height=self.distribution / factor, shape=shape)
                blocks.append(block)
        return sorted(blocks, key=lambda block: block.startPoint)

    def to_json(self) -> str:
        return json.dumps({
            'distribution': self.distribution,
            'startDistribution': self.startDistribution,
            'duration': self.duration,
            'startOffset': self.startOffset,
            'durationOffset': self.durationOffset,
            'developmentPeriods': self.developmentPeriods
        })

    def __str__(self) -> str:
        return f"PatternSlice: (Distribution: {self.distribution}, Start Distribution: {self.startDistribution}, Duration: {self.duration}, Start Offset: {self.startOffset}, Duration Offset: {self.durationOffset}, Development Periods: {self.developmentPeriods})"

class BlockShape(str, Enum):
    LTRIANGLE = "LTRIANGLE"
    RTRIANGLE = "RTRIANGLE"
    RECTANGLE = "RECTANGLE"

class PatternBlock:
    def __init__(self, pattern: str, sliceNumber: int = 0, displayLevel: int = 0, startPoint: int = 0, endPoint: int = 0, height: float = 0, shape: BlockShape = BlockShape.RECTANGLE):
        self.pattern = pattern
        self.sliceNumber = sliceNumber
        self.displayLevel = displayLevel
        self.startPoint = startPoint
        self.endPoint = endPoint
        self.height = height
        self.ultimateValue = height
        self.shape = shape

    def generate_polygon(self, colour: str = "blue", stroke: str = "black", yAxis: int = 0, height: int = 50) -> str:
        points = self._generate_points(yAxis, height)
        return f'<polygon vector-effect="non-scaling-stroke" stroke-width="1" points="{points}" fill="{colour}" stroke="{stroke}" />'

    def _generate_points(self, yAxis: int, height: int) -> str:
        if self.shape == BlockShape.RECTANGLE:
            return f"{self.startPoint},{yAxis} {self.endPoint + 1},{yAxis} {self.endPoint + 1},{yAxis + height} {self.startPoint},{yAxis + height}"
        elif self.shape == BlockShape.LTRIANGLE:
            return f"{self.startPoint},{yAxis} {self.endPoint + 1},{yAxis} {self.endPoint + 1},{yAxis + height}"
        elif self.shape == BlockShape.RTRIANGLE:
            return f"{self.startPoint},{yAxis} {self.startPoint},{yAxis + height} {self.endPoint + 1},{yAxis + height}"
        return ""

    def __str__(self) -> str:
        return f"PatternBlock with Pattern: {self.pattern}, Slice Number: {self.sliceNumber}, Display Level: {self.displayLevel}, Start Point: {self.startPoint}, End Point: {self.endPoint}, Height: {self.height}, Ultimate Value: {self.ultimateValue}, Shape: {self.shape.name}"

class PatternEvaluator:
    def __init__(self, patternBlocks: List[PatternBlock]):
        if not all(isinstance(block, PatternBlock) for block in patternBlocks):
            raise TypeError("All elements must be instances of PatternBlock")
        self.patternBlocks = patternBlocks

    def apply_ultimate_value(self, ultimateValue: float):
        for block in self.patternBlocks:
            block.ultimateValue = block.height * ultimateValue

    def get_pattern_blocks_less_than(self, point: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.startPoint < point]

    def get_pattern_blocks_greater_than(self, point: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.startPoint > point]

    def get_pattern_blocks_between(self, startPoint: int, endPoint: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if startPoint <= block.startPoint < endPoint]

    def evaluate_written_blocks(self, latestWrittenSlice: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.sliceNumber <= latestWrittenSlice]

    def evaluate_unwritten_blocks(self, latestWrittenSlice: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.sliceNumber > latestWrittenSlice]

    def evaluate_lic_blocks(self, latestWrittenSlice: int) -> List[PatternBlock]:
        endPointOfLatestSlice = self.get_earliest_end_point_of_slice(latestWrittenSlice)
        return [block for block in self.patternBlocks if block.sliceNumber <= latestWrittenSlice and block.endPoint <= endPointOfLatestSlice]

    def evaluate_lrc_blocks(self, latestWrittenSlice: int) -> List[PatternBlock]:
        endPointOfLatestSlice = self.get_earliest_end_point_of_slice(latestWrittenSlice)
        return [block for block in self.patternBlocks if block.sliceNumber <= latestWrittenSlice and block.endPoint > endPointOfLatestSlice]

    def evaluate_upr_blocks(self, latestWrittenSlice: int) -> List[PatternBlock]:
        endPointOfLatestSlice = self.get_earliest_end_point_of_slice(latestWrittenSlice)
        return [block for block in self.patternBlocks if block.endPoint > endPointOfLatestSlice]

    def get_earliest_end_point_of_slice(self, sliceNumber: int) -> Optional[int]:
        sliceBlocks = [block for block in self.patternBlocks if block.sliceNumber == sliceNumber]
        return min((block.endPoint for block in sliceBlocks), default=None)

    def get_earliest_start_point_of_slice(self, sliceNumber: int) -> Optional[int]:
        sliceBlocks = [block for block in self.patternBlocks if block.sliceNumber == sliceNumber]
        return min((block.startPoint for block in sliceBlocks), default=None)

    def get_latest_end_point_of_slice(self, sliceNumber: int) -> Optional[int]:
        sliceBlocks = [block for block in self.patternBlocks if block.sliceNumber == sliceNumber]
        return max((block.endPoint for block in sliceBlocks), default=None)

    def get_latest_start_point_of_slice(self, sliceNumber: int) -> Optional[int]:
        sliceBlocks = [block for block in self.patternBlocks if block.sliceNumber == sliceNumber]
        return max((block.startPoint for block in sliceBlocks), default=None)

    def save_to_file(self, filename: str):
        with open(filename, 'w') as file:
            json.dump([block.__dict__ for block in self.patternBlocks], file)

    @classmethod
    def load_from_file(cls, filename: str) -> 'PatternEvaluator':
        with open(filename, 'r') as file:
            blocksData = json.load(file)
            patternBlocks = [PatternBlock(**blockData) for blockData in blocksData]
            return cls(patternBlocks)

    @staticmethod
    def create_svg(patternBlocks: List[PatternBlock], latestWrittenSlice: Optional[int] = None, dayCut: Optional[int] = None, sliceHeights: dict[int, int] = None, heightScale: float = 0.7, preColour: str = "white", colour: str = "blue", condition: str = "or") -> str:
        minPoint, maxPoint = PatternEvaluator.find_min_max_points(patternBlocks)
        width = maxPoint - minPoint
        if sliceHeights is None:
            largestHeightPerDisplayLevel = PatternEvaluator.find_largest_height_per_display_level(patternBlocks)
            sliceHeights = cumulative_sum(scale_vector_to_sum(largestHeightPerDisplayLevel, (maxPoint - minPoint)*heightScale))
        height, svgElements = PatternEvaluator.generate_svg_elements(patternBlocks, latestWrittenSlice, dayCut, sliceHeights, preColour, colour, condition)
        return f'<svg width="100%" height="auto" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">{"".join(svgElements)}</svg>'

    @staticmethod
    def generate_svg_elements(patternBlocks: List[PatternBlock], latestWrittenSlice: Optional[int], dayCut: Optional[int], sliceHeights: dict[int, int], preColour: str, colour: str, condition: str) -> Tuple[int, List[str]]:
        svgElements = []
        sliceHeight = 0
        yAxis = 0
        height = 0
        for block in patternBlocks:
            blockColour = PatternEvaluator.determine_block_colour(block, latestWrittenSlice, dayCut, preColour, colour, condition)
            if block.displayLevel == 0:
                sliceHeight = sliceHeights[block.displayLevel]
                yAxis = 0
                if sliceHeights[block.displayLevel] > height:
                    height = sliceHeights[block.displayLevel]
            else:
                sliceHeight = sliceHeights[block.displayLevel]-sliceHeights[block.displayLevel-1]
                yAxis = sliceHeights[block.displayLevel-1]
                if sliceHeights[block.displayLevel] > height:
                    height = sliceHeights[block.displayLevel]
            element = block.generate_polygon(blockColour, yAxis=yAxis, height=sliceHeight)
            svgElements.append(element)
        return height, svgElements

    @staticmethod
    def determine_block_colour(block: PatternBlock, latestWrittenSlice: Optional[int], dayCut: Optional[int], preColour: str, colour: str, condition: str) -> str:
        blockColour = colour
        if condition == "and":
            if (latestWrittenSlice is not None and block.sliceNumber <= latestWrittenSlice) and (dayCut is not None and block.startPoint <= dayCut):
                blockColour = preColour
        elif condition == "or":
            if (latestWrittenSlice is not None and block.sliceNumber <= latestWrittenSlice) or (dayCut is not None and block.startPoint <= dayCut):
                blockColour = preColour
        return blockColour

    @staticmethod
    def find_min_max_points(patternBlocks: List[PatternBlock]) -> Tuple[Optional[int], Optional[int]]:
        if not patternBlocks:
            return None, None
        minStartPoint = min(block.startPoint for block in patternBlocks)
        maxEndPoint = max(block.endPoint for block in patternBlocks)
        return minStartPoint, maxEndPoint

    @staticmethod
    def sum_ultimate_values(patternBlocks: List[PatternBlock]) -> float:
        return sum(block.ultimateValue for block in patternBlocks)

    @staticmethod
    def sum_block_heights(patternBlocks: List['PatternBlock']) -> float:
        return sum(block.height for block in patternBlocks)

    @staticmethod
    def find_largest_height_per_display_level(patternBlocks: List[PatternBlock]) -> dict:
        largestHeights = {}
        for block in patternBlocks:
            if block.displayLevel not in largestHeights or block.height > largestHeights[block.displayLevel]:
                largestHeights[block.displayLevel] = block.height
        return largestHeights

    def __str__(self) -> str:
        return f"PatternEvaluator with {len(self.patternBlocks)} blocks"

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
    for block in evaluator.patternBlocks:
        print(block)
        
    evaluator.apply_ultimate_value(136.5)

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
