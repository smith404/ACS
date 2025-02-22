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
from pattern_slice import PatternSlice

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
            if slice.startDistribution is not None and slice.startDistribution != 0:
                displayLevel += 1
            if slice.distribution is not None and slice.distribution != 0:
                displayLevel += 1
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

    @classmethod
    def load_from_file(cls, filename: str) -> 'Pattern':
        with open(filename, 'r') as file:
            data = json.load(file)
            pattern = cls(duration=data['duration'])
            pattern.identifier = data['identifier']
            pattern.slices = [PatternSlice(**sliceData) for sliceData in data['slices']]
            return pattern

    def to_json(self) -> str:
        return json.dumps({
            'identifier': str(self.identifier),
            'duration': self.duration,
            'slices': [slice.__dict__ for slice in self.slices]
        })

    @classmethod
    def from_json(cls, json_str: str) -> 'Pattern':
        data = json.loads(json_str)
        pattern = cls(duration=data['duration'])
        pattern.identifier = data['identifier']
        pattern.slices = [PatternSlice.from_json(json.dumps(sliceData)) for sliceData in data['slices']]
        return pattern

    def __str__(self) -> str:
        return f"Pattern with {len(self.slices)} slices and duration {self.duration}"
