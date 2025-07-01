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
import os
from pattern_block import PatternBlock
from pattern_element import PatternElement

class Pattern:
    def __init__(self, duration: int = 360):
        self.identifier = uuid.uuid4()
        self.elements: List[PatternElement] = []
        self.duration = duration

    def add_element(self, patternElement: 'PatternElement'):
        if isinstance(patternElement, PatternElement):
            patternElement.set_duration(self.duration)
            self.elements.append(patternElement)
        else:
            raise TypeError("Expected a PatternElement instance")

    def set_identifier(self, identifier: str):
        self.identifier = identifier

    def set_duration(self, duration: int):
        self.duration = duration
        for element in self.elements:
            element.set_duration(duration)

    def align_element_periods(self, developmentPeriods: Optional[int] = None):
        if developmentPeriods is None or developmentPeriods == 0:
            developmentPeriods = len(self.elements)
        for index, element in enumerate(self.elements):
            element.set_development_periods(developmentPeriods)
            element.set_duration_offset(self.duration / developmentPeriods)
            element.set_start_offset(index * (self.duration / developmentPeriods))

    def delete_element(self, patternElement: Optional['PatternElement'] = None):
        if patternElement in self.elements:
            self.elements.remove(patternElement)
        else:
            raise ValueError("PatternElement not found in elements")

    def expand(self):
        for element in self.elements:
            if element.startDistribution != 0:
                element.iterate_start_periods()
            if element.distribution != 0:
                element.iterate_development_periods()

    def distribute_remaining(self):
        totalDistribution = sum(element.distribution + element.startDistribution for element in self.elements)
        if totalDistribution < 1:
            remaining = 1 - totalDistribution
            for element in self.elements:
                element.distribution += remaining / len(self.elements)

    def set_all_distributions_to_zero(self):
        for element in self.elements:
            element.distribution = 0

    def set_all_start_distributions_to_zero(self):
        for element in self.elements:
            element.startDistribution = 0

    def set_pattern_to_zero(self):
        self.set_all_start_distributions_to_zero()
        self.set_all_distributions_to_zero()

    def check_distribution(self) -> bool:
        totalDistribution = sum(element.distribution + element.startDistribution for element in self.elements)
        return totalDistribution == 1

    def check_durations(self) -> bool:
        return all(element.duration == self.duration for element in self.elements)

    def get_all_pattern_blocks(self) -> List['PatternBlock']:
        blocks = []
        writenElement = 0
        for index, element in enumerate(self.elements):
            blocks.extend(element.get_pattern_blocks(patternId=self.identifier, elementNumber=index, writenElement=writenElement))
            if element.startDistribution is not None and element.startDistribution != 0:
                writenElement += 1
            if element.distribution is not None and element.distribution != 0:
                writenElement += 1
        return blocks

    def display(self):
        print(self)
        for element in self.elements:
            print(element)

    def save_to_file(self, filename: str):
        with open(filename, 'w') as file:
            json.dump({
                'identifier': str(self.identifier),
                'duration': self.duration,
                'elements': [element.__dict__ for element in self.elements]
            }, file)

    import os

    @classmethod
    def load_from_file(cls, filename: str) -> 'Pattern':
        # Only allow filenames without path separators and with .json extension
        if not filename.endswith('.json') or os.path.basename(filename) != filename:
            raise ValueError("Invalid filename provided.")
        with open(filename, 'r') as file:
            data = json.load(file)
            pattern = cls(duration=data['duration'])
            pattern.identifier = data['identifier']
            pattern.elements = [PatternElement(**elementData) for elementData in data['elements']]
            return pattern

    def to_json(self) -> str:
        return json.dumps({
            'identifier': str(self.identifier),
            'duration': self.duration,
            'elements': [element.__dict__ for element in self.elements]
        })

    @classmethod
    def from_json(cls, json_str: str) -> 'Pattern':
        data = json.loads(json_str)
        pattern = cls(duration=data['duration'])
        pattern.identifier = data['identifier']
        pattern.elements = [PatternElement.from_json(json.dumps(elementData)) for elementData in data['elements']]
        return pattern

    def __str__(self) -> str:
        return f"Pattern with {len(self.elements)} elements and duration {self.duration}"
