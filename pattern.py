# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from typing import List, Optional
import json
import uuid
from pattern_block import PatternBlock
from pattern_element import PatternElement

class Pattern:
    def __init__(self, duration: int = 360):
        self.identifier = uuid.uuid4()
        self.slices: List[PatternElement] = []
        self.duration = duration

    def add_slice(self, patternElement: 'PatternElement'):
        if isinstance(patternElement, PatternElement):
            patternElement.set_duration(self.duration)
            self.slices.append(patternElement)
        else:
            raise TypeError("Expected a PatternElement instance")

    def set_identifier(self, identifier: str):
        self.identifier = identifier

    def set_duration(self, duration: int):
        self.duration = duration
        for element in self.slices:
            element.set_duration(duration)

    def align_slice_periods(self, developmentPeriods: Optional[int] = None):
        if developmentPeriods is None or developmentPeriods == 0:
            developmentPeriods = len(self.slices)
        for index, element in enumerate(self.slices):
            element.set_development_periods(developmentPeriods)
            element.set_duration_offset(self.duration / developmentPeriods)
            element.set_start_offset(index * (self.duration / developmentPeriods))

    def delete_slice(self, patternElement: Optional['PatternElement'] = None):
        if patternElement in self.slices:
            self.slices.remove(patternElement)
        else:
            raise ValueError("PatternElement not found in slices")

    def expand(self):
        for element in self.slices:
            if element.startDistribution != 0:
                element.iterate_start_periods()
            if element.distribution != 0:
                element.iterate_development_periods()

    def distribute_remaining(self):
        totalDistribution = sum(element.distribution + element.startDistribution for element in self.slices)
        if totalDistribution < 1:
            remaining = 1 - totalDistribution
            for element in self.slices:
                element.distribution += remaining / len(self.slices)

    def set_all_distributions_to_zero(self):
        for element in self.slices:
            element.distribution = 0

    def set_all_start_distributions_to_zero(self):
        for element in self.slices:
            element.startDistribution = 0

    def set_pattern_to_zero(self):
        self.set_all_start_distributions_to_zero()
        self.set_all_distributions_to_zero()

    def check_distribution(self) -> bool:
        totalDistribution = sum(element.distribution + element.startDistribution for element in self.slices)
        return totalDistribution == 1

    def check_durations(self) -> bool:
        return all(element.duration == self.duration for element in self.slices)

    def get_all_pattern_blocks(self) -> List['PatternBlock']:
        blocks = []
        displayLevel = 0
        for index, element in enumerate(self.slices):
            blocks.extend(element.get_pattern_blocks(patternId=self.identifier, sliceNumber=index, displayLevel=displayLevel))
            if element.startDistribution is not None and element.startDistribution != 0:
                displayLevel += 1
            if element.distribution is not None and element.distribution != 0:
                displayLevel += 1
        return blocks

    def display(self):
        print(self)
        for element in self.slices:
            print(element)

    def save_to_file(self, filename: str):
        with open(filename, 'w') as file:
            json.dump({
                'identifier': str(self.identifier),
                'duration': self.duration,
                'slices': [element.__dict__ for element in self.slices]
            }, file)

    @classmethod
    def load_from_file(cls, filename: str) -> 'Pattern':
        with open(filename, 'r') as file:
            data = json.load(file)
            pattern = cls(duration=data['duration'])
            pattern.identifier = data['identifier']
            pattern.slices = [PatternElement(**sliceData) for sliceData in data['slices']]
            return pattern

    def to_json(self) -> str:
        return json.dumps({
            'identifier': str(self.identifier),
            'duration': self.duration,
            'slices': [element.__dict__ for element in self.slices]
        })

    @classmethod
    def from_json(cls, json_str: str) -> 'Pattern':
        data = json.loads(json_str)
        pattern = cls(duration=data['duration'])
        pattern.identifier = data['identifier']
        pattern.slices = [PatternElement.from_json(json.dumps(sliceData)) for sliceData in data['slices']]
        return pattern

    def __str__(self) -> str:
        return f"Pattern with {len(self.slices)} slices and duration {self.duration}"
