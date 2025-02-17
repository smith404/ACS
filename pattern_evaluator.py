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
from pattern_block import PatternBlock, BlockShape

from utils import cumulative_sum, scale_vector_to_sum

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

    def sum_ultimate_values_by_points(self) -> dict[int, float]:
        value_sums = {}
        for block in self.patternBlocks:
            key = int(block.endPoint)
            if key not in value_sums:
                value_sums[key] = 0
            value_sums[key] += block.ultimateValue
        return value_sums

    def cumulative_sum_by_points(self, value_sums: dict[ int, float]) -> dict[int, float]:
        sorted_points = sorted(value_sums.keys())
        cumulative_sum = 0
        cumulative_sums = {}
        for point in sorted_points:
            cumulative_sum += value_sums[point]
            cumulative_sums[point] = cumulative_sum
        return cumulative_sums

    @classmethod
    def load_from_file(cls, filename: str) -> 'PatternEvaluator':
        with open(filename, 'r') as file:
            blocksData = json.load(file)
            patternBlocks = [PatternBlock(**blockData) for blockData in blocksData]
            return cls(patternBlocks)

    @staticmethod
    def create_svg(patternBlocks: List[PatternBlock], latestWrittenSlice: Optional[int] = None, dayCut: Optional[int] = None, sliceHeights: dict[int, int] = None, heightScale: float = 0.7, preColour: str = "white", colour: str = "lightblue", condition: str = "or", showText: bool = True) -> str:
        minPoint, maxPoint = PatternEvaluator.find_min_max_points(patternBlocks)
        width = maxPoint - minPoint
        if sliceHeights is None:
            largestHeightPerDisplayLevel = PatternEvaluator.find_largest_height_per_display_level(patternBlocks)
            sliceHeights = cumulative_sum(scale_vector_to_sum(largestHeightPerDisplayLevel, (maxPoint - minPoint)*heightScale))
        print(f"ShowText: {showText}")
        height, svgElements = PatternEvaluator.generate_svg_elements(patternBlocks, latestWrittenSlice, dayCut, sliceHeights, preColour, colour, condition, showText)
        return f'<svg height="100%" width="100%" viewBox="0 0 {width+5} {height+5}" xmlns="http://www.w3.org/2000/svg">{"".join(svgElements)}</svg>'

    @staticmethod
    def generate_svg_elements(patternBlocks: List[PatternBlock], latestWrittenSlice: Optional[int], dayCut: Optional[int], sliceHeights: dict[int, int], preColour: str, colour: str, condition: str, showText: bool) -> Tuple[int, List[str]]:
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
            element = block.generate_polygon(blockColour, yAxis=yAxis, height=sliceHeight, showText=showText)
            svgElements.append(element)
        return height, svgElements

    @staticmethod
    def determine_block_colour(block: PatternBlock, latestWrittenSlice: Optional[int], dayCut: Optional[int], preColour: str, colour: str, condition: str) -> str:
        blockColour = colour
        if condition == "and":
            if (latestWrittenSlice is not None and block.sliceNumber < latestWrittenSlice) and (dayCut is not None and block.startPoint < dayCut):
                blockColour = preColour
        elif condition == "or":
            if (latestWrittenSlice is not None and block.sliceNumber < latestWrittenSlice) or (dayCut is not None and block.startPoint < dayCut):
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

