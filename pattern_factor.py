from enum import Enum
from datetime import datetime, timedelta
from typing import List, Optional
import uuid
from dataclasses import dataclass


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
    """Represents a factor with incurred date, effective date, value and type."""
    incurred_date: datetime
    effective_date: datetime
    value: float
    factor_type: str


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
                 up_front_duration: int = 0, 
                 distribution_duration: int = 0):
        """
        Initialize a PatternFactor.
        
        Args:
            type_: The time period type
            up_front: The upfront value (default: 0.0)
            distribution: The distribution value (default: 0.0)
            up_front_duration: Duration for upfront distribution in days (default: 0)
            distribution_duration: Duration for distribution in days (default: 0)
        """
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
        """
        Create a PatternFactor with only distribution (no upfront).
        
        Args:
            type_: The time period type
            distribution: The distribution value
            distribution_duration: Duration for distribution in days
        """
        return cls(type_, 0.0, distribution, 0, distribution_duration)
    
    @staticmethod
    def get_normalized_duration(initial_date: Optional[datetime], duration: int) -> int:
        """
        Converts a duration in days (using 360-day year convention) to actual calendar days.
        
        Args:
            initial_date: Starting date for the calculation
            duration: Duration in 360-day convention
            
        Returns:
            Actual calendar days between initial_date and the calculated end date
        """
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
    
    def get_factors(self, start_date: datetime, use_calendar: bool, linear: bool) -> List[Factor]:
        """
        Generate factors based on the pattern configuration.
        
        Args:
            start_date: The starting date for factor generation
            use_calendar: Whether to use calendar normalization
            linear: Whether to use linear distribution
            
        Returns:
            List of Factor objects
        """
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
    
    def _up_front_factors(self, incurred_date: datetime) -> List[Factor]:
        """
        Generate upfront factors.
        
        Args:
            incurred_date: The date when the cost was incurred
            
        Returns:
            List of upfront Factor objects
        """
        factors = []
        
        if self.normalized_up_front_duration > 0:
            daily_factor = self.up_front / self.normalized_up_front_duration
            for i in range(self.normalized_up_front_duration):
                effective_date = incurred_date + timedelta(days=i)
                factors.append(Factor(incurred_date, effective_date, daily_factor, "UPFRONT"))
        
        return factors
    
    def _distribution_factors(self, incurred_date: datetime) -> List[Factor]:
        """
        Generate distribution factors.
        
        Args:
            incurred_date: The date when the cost was incurred
            
        Returns:
            List of distribution Factor objects
        """
        factors = []
        
        if self.normalized_distribution_duration > 0 and self.normalized_element_days > 0:
            daily_factor = self.distribution / self.normalized_distribution_duration / self.normalized_element_days
            
            for i in range(self.normalized_distribution_duration):
                effective_date = incurred_date + timedelta(days=i)
                
                if i == 0:
                    # First day gets half factor
                    factors.append(Factor(incurred_date, effective_date, daily_factor / 2, "DISTRIBUTION"))
                else:
                    factors.append(Factor(incurred_date, effective_date, daily_factor, "DISTRIBUTION"))
            
            # Last day gets half factor
            final_date = incurred_date + timedelta(days=self.normalized_distribution_duration)
            factors.append(Factor(incurred_date, final_date, daily_factor / 2, "DISTRIBUTION"))
        
        return factors
    
    def __str__(self) -> str:
        """String representation of the PatternFactor."""
        return (f"PatternFactor(uuid={self.uuid}, type={self.type.name}, "
                f"element_days={self.element_days}, up_front={self.up_front}, "
                f"distribution={self.distribution})")
    
    def __repr__(self) -> str:
        """Detailed string representation of the PatternFactor."""
        return self.__str__()


# Example usage
if __name__ == "__main__":
    # Create a monthly pattern factor
    pattern = PatternFactor(Type.MONTH, up_front=100.0, distribution=500.0, 
                          up_front_duration=30, distribution_duration=90)
    
    print(f"Pattern: {pattern}")
    print(f"Element days: {pattern.element_days}")
    print(f"Type default days: {pattern.type.get_default_days()}")
    
    # Create a simple distribution-only pattern
    simple_pattern = PatternFactor.create_simple(Type.WEEK, 200.0, 14)
    print(f"Simple pattern: {simple_pattern}")
    
    # Generate factors
    start_date = datetime(2025, 1, 1)
    factors = pattern.get_factors(start_date, use_calendar=True, linear=False)
    print(f"Generated {len(factors)} factors")
