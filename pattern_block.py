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

class BlockShape(str, Enum):
    LTRIANGLE = "LTRIANGLE"
    RTRIANGLE = "RTRIANGLE"
    RECTANGLE = "RECTANGLE"

class PatternBlock:
    def __init__(self, pattern: str, sliceNumber: int = 0, displayLevel: int = 0, startPoint: int = 0, endPoint: int = 0, height: float = 0, value: float = 0, shape: BlockShape = BlockShape.RECTANGLE):
        self.pattern = pattern
        self.sliceNumber = sliceNumber
        self.displayLevel = displayLevel
        self.startPoint = startPoint
        self.endPoint = endPoint
        self.height = height
        self.ultimateValue = value
        self.shape = shape

    def generate_polygon(self, colour: str = "blue", stroke: str = "black", yAxis: int = 0, height: int = 50, showText: bool = False, showValue: bool = False) -> str:
        points = self._generate_points(yAxis, height)
        polygon = f'<polygon vector-effect="non-scaling-stroke" stroke-width="1" points="{points}" fill="{colour}" stroke="{stroke}" />'
        if showText:
            text_x = (self.startPoint + self.endPoint + 1) / 2
            text_y = yAxis + height / 2
            if showValue:
                rounded_height = round(self.ultimateValue, 4)
            else:
                rounded_height = round(self.height, 4)
            text = f'<text x="{text_x}" y="{text_y}" fill="black" font-size="18" text-anchor="middle" alignment-baseline="middle">{rounded_height}</text>'
            return polygon + text
        return polygon

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

    def __str__(self) -> str:
        return f"PatternBlock with Pattern: {self.pattern}, Slice Number: {self.sliceNumber}, Display Level: {self.displayLevel}, Start Point: {self.startPoint}, End Point: {self.endPoint}, Height: {self.height}, Ultimate Value: {self.ultimateValue}, Shape: {self.shape.name}"
