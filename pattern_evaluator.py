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

from utils import cumulative_sum, scale_vector_to_sum

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

    @classmethod
    def from_json(cls, json_str: str) -> 'PatternBlock':
        data = json.loads(json_str)
        return cls(
            pattern=data['pattern'],
            sliceNumber=data['sliceNumber'],
            displayLevel=data['displayLevel'],
            startPoint=data['startPoint'],
            endPoint=data['endPoint'],
            height=data['height'],
            shape=BlockShape(data['shape'])
        )

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

    def to_json(self) -> str:
        return json.dumps({
            "pattern": self.pattern,
            "sliceNumber": self.sliceNumber,
            "displayLevel": self.displayLevel,
            "startPoint": self.startPoint,
            "endPoint": self.endPoint,
            "height": self.height,
            "ultimateValue": self.ultimateValue,
            "shape": self.shape.value
        })

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

