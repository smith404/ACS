# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.
 
import json
from typing import List
from pattern_evaluator import PatternBlock, BlockShape

class PatternSlice:
    def __init__(self, distribution: float = 0, startDistribution: float = 0, skew: float = 0, duration: int = 0, startOffset: int = 0, durationOffset: int = 0, developmentPeriods: int = 0):
        self.distribution = distribution
        self.startDistribution = startDistribution
        self.skew = skew
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
        if self.startDistribution != None and self.startDistribution != 0:
            for index in range(self.developmentPeriods):
                shape = BlockShape.FIRST
                startPoint = self.startOffset + (index * self.durationOffset)
                endPoint = self.startOffset + ((index + 1) * self.durationOffset) - 1
                block = PatternBlock(patternId, sliceNumber=sliceNumber, displayLevel=displayLevel, startPoint=startPoint, endPoint=endPoint, proportion=self.startDistribution / self.developmentPeriods, value=0, shape=shape)
                blocks.append(block)
            displayLevel = displayLevel + 1
        if self.distribution != None and self.distribution != 0:
            for index in range(0, self.developmentPeriods + 1):
                shape = BlockShape.LINEAR
                factor = self.developmentPeriods
                if index == 0 or index == self.developmentPeriods:
                    factor = factor * 2
                    if index == 0:
                        shape = BlockShape.INC_PROP
                    else:
                        shape = BlockShape.DEC_PROP
                startPoint = self.startOffset+(index * self.durationOffset)
                endPoint = (self.startOffset+((index + 1) * self.durationOffset)) - 1
                block = PatternBlock(patternId, sliceNumber=sliceNumber, displayLevel=displayLevel, startPoint=startPoint, endPoint=endPoint, proportion=self.distribution / factor, value=0, shape=shape)
                blocks.append(block)
        return sorted(blocks, key=lambda block: block.startPoint)

    def to_json(self) -> str:
        return json.dumps({
            'distribution': self.distribution,
            'startDistribution': self.startDistribution,
            'skew': self.skew,
            'duration': self.duration,
            'startOffset': self.startOffset,
            'durationOffset': self.durationOffset,
            'developmentPeriods': self.developmentPeriods
        })

    @classmethod
    def from_json(cls, json_str: str) -> 'PatternSlice':
        data = json.loads(json_str)
        return cls(
            distribution=data.get('distribution', 0),
            startDistribution=data.get('startDistribution', 0),
            skew=data.get('skew', 0),
            duration=data.get('duration', 0),
            startOffset=data.get('startOffset', 0),
            durationOffset=data.get('durationOffset', 0),
            developmentPeriods=data.get('developmentPeriods', 0)
        )

    def __str__(self) -> str:
        return f"PatternSlice: (Distribution: {self.distribution}, Start Distribution: {self.startDistribution}, Duration: {self.duration}, Start Offset: {self.startOffset}, Duration Offset: {self.durationOffset}, Development Periods: {self.developmentPeriods})"
