# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from enum import Enum
from datetime import date, timedelta
from typing import List, Optional
import uuid
from dataclasses import dataclass


class FactorType(Enum):
    UPFRONT = "UPFRONT"
    FIRST_DISTRIBUTED = "FIRST_DISTRIBUTED"
    DISTRIBUTED = "DISTRIBUTED"
    LAST_DISTRIBUTED = "LAST_DISTRIBUTED"


class Type(Enum):
    """Enum representing different time period types with their default days."""
    DAY = 1
    WEEK = 7
    MONTH = 30
    QUARTER = 90
    YEAR = 360
    
    def get_default_days(self) -> int:
        """Get the default number of days for this type."""
        return self.value


@dataclass
class Factor:
    """Represents a factor with incurred date, exposed date, value and type."""
    incurred_date: date
    exposed_date: date
    value: float
    factor_type: FactorType


class PatternFactor:
    """
    Represents a pattern element that defines how factors are distributed over time.
    Each element has a type (DAY, WEEK, MONTH, QUARTER, YEAR), an initial value,
    and a distribution value that gets spread over a duration.
    """
    
    DAYS_PER_YEAR = 360
    DAYS_PER_MONTH = 30
    MONTHS_PER_QUARTER = 3
    
    def __init__(self, 
                 type_: Type, 
                 up_front: float = 0.0, 
                 distribution: float = 0.0, 
                 up_front_duration: int = 1, 
                 distribution_duration: int = 1):
        distribution_duration = max(1, distribution_duration)
        up_front_duration = max(1, up_front_duration)

        self.uuid = str(uuid.uuid4())
        self.type = type_
        self.element_days = type_.get_default_days()
        self.normalized_element_days = self.element_days
        self.up_front = up_front
        self.distribution = distribution
        self.up_front_duration = up_front_duration
        self.normalized_up_front_duration = up_front_duration
        self.distribution_duration = distribution_duration
        self.normalized_distribution_duration = distribution_duration
    
    @classmethod
    def create_simple(cls, type_: Type, distribution: float, distribution_duration: int):
        distribution_duration = max(1, distribution_duration)
        return cls(type_, 0.0, distribution, 1, distribution_duration)
    
    @staticmethod
    def get_normalized_duration(initial_date: Optional[date], duration: int) -> int:
        if initial_date is None:
            return 0
        
        years = duration // PatternFactor.DAYS_PER_YEAR
        months = (duration % PatternFactor.DAYS_PER_YEAR) // PatternFactor.DAYS_PER_MONTH
        days = duration % PatternFactor.DAYS_PER_MONTH
        
        # Add years, months, and days to the initial date
        normalized_date = initial_date
        
        # Add years
        if years > 0:
            try:
                normalized_date = normalized_date.replace(year=normalized_date.year + years)
            except ValueError:
                # Handle leap year edge cases (Feb 29)
                normalized_date = normalized_date.replace(year=normalized_date.year + years, day=28)
        
        # Add months
        if months > 0:
            month = normalized_date.month + months
            year = normalized_date.year
            while month > 12:
                month -= 12
                year += 1
            try:
                normalized_date = normalized_date.replace(year=year, month=month)
            except ValueError:
                # Handle month-end edge cases
                if month == 2:
                    normalized_date = normalized_date.replace(year=year, month=month, day=28)
                elif month in [4, 6, 9, 11]:
                    normalized_date = normalized_date.replace(year=year, month=month, day=30)
                else:
                    normalized_date = normalized_date.replace(year=year, month=month, day=31)
        
        # Add remaining days
        if days > 0:
            normalized_date = normalized_date + timedelta(days=days)
        
        return (normalized_date - initial_date).days
    
    def get_factors(self, start_date: date, use_calendar: bool = True) -> List[Factor]:
        factors = []
        
        if use_calendar:
            self.normalized_element_days = self.get_normalized_duration(start_date, self.element_days)
            self.normalized_up_front_duration = self.get_normalized_duration(start_date, self.up_front_duration)
            self.normalized_distribution_duration = self.get_normalized_duration(start_date, self.distribution_duration)
        
        factors.extend(self._up_front_factors(start_date))
        
        for i in range(self.normalized_element_days):
            # Add distribution factors for each element day
            factors.extend(self._distribution_factors(start_date + timedelta(days=i)))
        
        return factors
    
    def _up_front_factors(self, incurred_date: date) -> List[Factor]:
        factors = []
        
        if self.normalized_up_front_duration > 0:
            daily_factor = self.up_front / self.normalized_up_front_duration
            for i in range(self.normalized_up_front_duration):
                effective_date = incurred_date + timedelta(days=i)
                factors.append(Factor(incurred_date, effective_date, daily_factor, FactorType.UPFRONT))
        
        return factors
    
    def _distribution_factors(self, incurred_date: date) -> List[Factor]:
        factors = []
        
        if self.normalized_distribution_duration > 0 and self.normalized_element_days > 0:
            daily_factor = self.distribution / self.normalized_distribution_duration / self.normalized_element_days
            
            for i in range(self.normalized_distribution_duration):
                effective_date = incurred_date + timedelta(days=i)
                
                if i == 0:
                    # First day gets half factor
                    factors.append(Factor(incurred_date, effective_date, daily_factor / 2, FactorType.FIRST_DISTRIBUTED))
                else:
                    factors.append(Factor(incurred_date, effective_date, daily_factor, FactorType.DISTRIBUTED))
            
            # Last day gets half factor
            final_date = incurred_date + timedelta(days=self.normalized_distribution_duration)
            factors.append(Factor(incurred_date, final_date, daily_factor / 2, FactorType.LAST_DISTRIBUTED))
        
        return factors
    
    def __str__(self) -> str:
        """String representation of the PatternFactor."""
        return (f"Type={self.type.name}, element_days={self.element_days}, "
                f"up_front={self.up_front}, up_front_duration={self.up_front_duration}, "
                f"distribution={self.distribution} distribution_duration={self.distribution_duration}")
    
    def __repr__(self) -> str:
        """Detailed string representation of the PatternFactor."""
        return self.__str__()

