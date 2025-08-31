# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from pattern_factor import PatternFactor

from typing import List, Optional
import json
import uuid
from datetime import date, timedelta

class Pattern:
    def __init__(self):
        self.identifier = uuid.uuid4()
        self.elements: List[PatternFactor] = []

    def add_element(self, pattern_factor: 'PatternFactor'):
        if isinstance(pattern_factor, PatternFactor):
            self.elements.append(pattern_factor)
        else:
            raise TypeError("Expected a PatternFactor instance")

    def set_identifier(self, identifier: str):
        self.identifier = identifier

    def delete_element(self, pattern_factor: Optional['PatternFactor'] = None):
        if pattern_factor in self.elements:
            self.elements.remove(pattern_factor)
        else:
            raise ValueError("PatternFactor not found in elements")

    def check_distribution(self) -> bool:
        total_distribution = sum(element.distribution + element.up_front for element in self.elements)
        return total_distribution == 1

    def display(self):
        print(self)
        for element in self.elements:
            print(element)

    def save_to_file(self, filename: str):
        with open(filename, 'w') as file:
            json.dump({
                'identifier': str(self.identifier),
                'elements': [element.__dict__ for element in self.elements]
            }, file)

    def get_factors(self, start_date: date, use_calendar: bool = False) -> List:
        if start_date is None:
            return []
        
        all_factors = []
        current_start_date = start_date
        
        for element in self.elements:
            factors = element.get_factors(current_start_date, use_calendar)
            all_factors.extend(factors)
            
            # Increment start date by the element's normalized_element_days
            current_start_date = current_start_date + timedelta(days=element.normalized_element_days)
        
        return all_factors

    def __str__(self) -> str:
        return f"Pattern with {len(self.elements)} elements"
