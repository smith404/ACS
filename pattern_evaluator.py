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
from typing import List, Optional, Tuple
from pattern_block import PatternBlock
from utils import cumulative_sum, scale_vector_to_sum

class PatternEvaluator:
    def __init__(self, patternBlocks: List[PatternBlock]):
        if not all(isinstance(block, PatternBlock) for block in patternBlocks):
            raise TypeError("All elements must be instances of PatternBlock")
        self.patternBlocks = patternBlocks

    def apply_ultimate_value(self, ultimateValue: float):
        for block in self.patternBlocks:
            block.ultimateValue = block.proportion * ultimateValue

    def get_pattern_blocks_less_than(self, point: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.startPoint < point]

    def get_pattern_blocks_greater_than(self, point: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.startPoint > point]

    def get_pattern_blocks_between(self, startPoint: int, endPoint: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if startPoint <= block.startPoint < endPoint]

    def evaluate_written_blocks(self, latestWrittenElement: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.elementNumber < latestWrittenElement]

    def evaluate_unwritten_blocks(self, latestWrittenElement: int) -> List[PatternBlock]:
        return [block for block in self.patternBlocks if block.elementNumber >= latestWrittenElement]

    def evaluate_lic_blocks(self, latestWrittenElement: int) -> List[PatternBlock]:
        endPointOfLatestElement = self.get_earliest_start_point_of_element(latestWrittenElement)
        return [block for block in self.patternBlocks if block.elementNumber < latestWrittenElement and block.endPoint < endPointOfLatestElement]

    def evaluate_lrc_blocks(self, latestWrittenElement: int) -> List[PatternBlock]:
        endPointOfLatestElement = self.get_earliest_start_point_of_element(latestWrittenElement)
        return [block for block in self.patternBlocks if block.elementNumber >= latestWrittenElement and block.endPoint >= endPointOfLatestElement]

    def evaluate_upr_blocks(self, latestWrittenElement: int) -> List[PatternBlock]:
        endPointOfLatestElement = self.get_earliest_start_point_of_element(latestWrittenElement)
        return [block for block in self.patternBlocks if block.endPoint >= endPointOfLatestElement]

    def get_earliest_start_point_of_element(self, elementNumber: int) -> Optional[int]:
        if elementNumber > max((block.elementNumber for block in self.patternBlocks)):
            # Special case that we can call this beyond the last element for LIC, LRC and UPR
            elementNumber = max((block.elementNumber for block in self.patternBlocks))
            return self.get_earliest_end_point_of_element(elementNumber) + 1
        elementBlocks = [block for block in self.patternBlocks if block.elementNumber == elementNumber]
        return min((block.startPoint for block in elementBlocks), default=None)

    def get_latest_start_point_of_element(self, elementNumber: int) -> Optional[int]:
        elementBlocks = [block for block in self.patternBlocks if block.elementNumber == elementNumber]
        return max((block.startPoint for block in elementBlocks), default=None)

    def get_earliest_end_point_of_element(self, elementNumber: int) -> Optional[int]:
        elementBlocks = [block for block in self.patternBlocks if block.elementNumber == elementNumber]
        return min((block.endPoint for block in elementBlocks), default=None)

    def get_latest_end_point_of_element(self, elementNumber: int) -> Optional[int]:
        elementBlocks = [block for block in self.patternBlocks if block.elementNumber == elementNumber]
        return max((block.endPoint for block in elementBlocks), default=None)

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
    def create_svg(patternBlocks: List[PatternBlock], latestWrittenElement: Optional[int] = None, dayCut: Optional[int] = None, elementHeights: dict[int, int] = None, heightScale: float = 0.7, preColour: str = "white", colour: str = "lightblue", condition: str = "or", showText: bool = True, showValue: bool = False) -> str:
        minPoint, maxPoint = PatternEvaluator.find_min_max_points(patternBlocks)
        width = maxPoint - minPoint
        if elementHeights is None:
            largestHeightPerDisplayLevel = PatternEvaluator.find_largest_height_per_display_level(patternBlocks)
            elementHeights = cumulative_sum(scale_vector_to_sum(largestHeightPerDisplayLevel, (maxPoint - minPoint)*heightScale))
        height, svgElements = PatternEvaluator.generate_svg_elements(patternBlocks, latestWrittenElement, dayCut, elementHeights, preColour, colour, condition, showText, showValue)
        return f'<svg height="100%" width="100%" viewBox="0 0 {width+5} {height+5}" xmlns="http://www.w3.org/2000/svg">{"".join(svgElements)}</svg>'

    @staticmethod
    def generate_svg_elements(patternBlocks: List[PatternBlock], latestWrittenElement: Optional[int], dayCut: Optional[int], elementHeights: dict[int, int], preColour: str, colour: str, condition: str, showText: bool, showValue: bool) -> Tuple[int, List[str]]:
        svgElements = []
        elementHeight = 0
        yAxis = 0
        height = 0
        for block in patternBlocks:
            blockColour = PatternEvaluator.determine_block_colour(block, latestWrittenElement, dayCut, preColour, colour, condition)
            if block.displayLevel == 0:
                elementHeight = elementHeights[block.displayLevel]
                yAxis = 0
                if elementHeights[block.displayLevel] > height:
                    height = elementHeights[block.displayLevel]
            else:
                elementHeight = elementHeights[block.displayLevel]-elementHeights[block.displayLevel-1]
                yAxis = elementHeights[block.displayLevel-1]
                if elementHeights[block.displayLevel] > height:
                    height = elementHeights[block.displayLevel]
            element = block.generate_polygon(blockColour, yAxis=yAxis, height=elementHeight, showText=showText, showValue=showValue)
            svgElements.append(element)
        return height, svgElements

    @staticmethod
    def determine_block_colour(block: PatternBlock, latestWrittenElement: Optional[int], dayCut: Optional[int], preColour: str, colour: str, condition: str) -> str:
        blockColour = preColour
        if condition == "t":
            if (latestWrittenElement is not None and block.elementNumber < latestWrittenElement):
                blockColour = colour
        if condition == "b":
            if (latestWrittenElement is not None and block.elementNumber >= latestWrittenElement):
                blockColour = colour
        if condition == "l":
            if (dayCut is not None and block.startPoint < dayCut):
                blockColour = colour
        if condition == "r":
            if (dayCut is not None and block.startPoint >= dayCut):
                blockColour = colour
        elif condition == "tl":
            if (latestWrittenElement is not None and block.elementNumber < latestWrittenElement) and (dayCut is not None and block.startPoint < dayCut):
                blockColour = colour
        elif condition == "tr":
            if (latestWrittenElement is not None and block.elementNumber < latestWrittenElement) and (dayCut is not None and block.startPoint >= dayCut):
                blockColour = colour
        elif condition == "bl":
            if (latestWrittenElement is not None and block.elementNumber >= latestWrittenElement) and (dayCut is not None and block.startPoint < dayCut):
                blockColour = colour
        elif condition == "br":
            if (latestWrittenElement is not None and block.elementNumber >= latestWrittenElement) and (dayCut is not None and block.startPoint >= dayCut):
                blockColour = colour
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
        return sum(block.proportion for block in patternBlocks)

    @staticmethod
    def find_largest_height_per_display_level(patternBlocks: List[PatternBlock]) -> dict:
        largestHeights = {}
        for block in patternBlocks:
            if block.displayLevel not in largestHeights or block.proportion > largestHeights[block.displayLevel]:
                largestHeights[block.displayLevel] = block.proportion
        return largestHeights

    def __str__(self) -> str:
        return f"PatternEvaluator with {len(self.patternBlocks)} blocks"

