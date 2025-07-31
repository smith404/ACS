# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from typing import List, Optional
from datetime import date, timedelta
from dataclasses import dataclass
from pattern_factor import Factor, Type
from calendar_factory import CalendarFactory, TimeUnit


@dataclass
class ExposureMatrixEntry:
    """Represents an entry in the exposure matrix with incurred and exposure date buckets and sum."""
    incurred_date_bucket: date
    exposure_date_bucket: date
    sum: float


@dataclass
class ExposureVectorEntry:
    """Represents an entry in an exposure vector with date bucket and sum."""
    date_bucket: date
    sum: float


@dataclass
class DateRange:
    """Represents a date range with start and end dates."""
    start: date
    end: date
    
    def is_valid(self) -> bool:
        return self.start <= self.end
    
    def contains(self, target_date: date) -> bool:
        return self.start <= target_date <= self.end


class ExposureMatrix:
    """
    Represents an exposure matrix that aggregates factors into date buckets for incurred and exposure dates.
    Provides functionality to generate matrix tables and vector summaries with configurable precision.
    """
    
    EXPOSURE_HEADER = "Exp x Inc"
    COLUMN_WIDTH = 12
    ZERO_THRESHOLD = 1e-10
    
    def __init__(self, factors: List[Factor],
                 incurred_bucket_dates: List[date], 
                 exposure_bucket_dates: List[date], 
                 to_end: bool = True):
        """
        Initialize the ExposureMatrix.
        
        Args:
            factors: List of Factor objects
            incurred_bucket_dates: List of incurred bucket end dates
            exposure_bucket_dates: List of exposure bucket end dates
            to_end: If True, use end dates for buckets; if False, use start dates
        """
        self._validate_constructor_inputs(factors, incurred_bucket_dates, exposure_bucket_dates)
        
        self.factors = factors
        # Store original bucket dates for matrix generation
        self._original_incurred_dates = sorted(incurred_bucket_dates)
        self._original_exposure_dates = sorted(exposure_bucket_dates)
        self.to_end = to_end
        
        # Generate the matrix entries by aggregating factors
        self.entries = self._generate_matrix_entries()
        
        # Filter out bucket dates with no non-zero values
        self.incurred_bucket_dates = self._get_active_incurred_buckets()
        self.exposure_bucket_dates = self._get_active_exposure_buckets()
    
    def _generate_matrix_entries(self) -> List[ExposureMatrixEntry]:
        """Generate matrix entries by aggregating factors into buckets."""
        entries = []
        
        # Create a dictionary to accumulate sums for each bucket combination
        bucket_sums = {}
        
        for factor in self.factors:
            incurred_bucket = self._get_bucket_date(factor.incurred_date, self._original_incurred_dates)
            exposure_bucket = self._get_bucket_date(factor.exposed_date, self._original_exposure_dates)
            
            if incurred_bucket is not None and exposure_bucket is not None:
                key = (incurred_bucket, exposure_bucket)
                bucket_sums[key] = bucket_sums.get(key, 0.0) + factor.value
        
        # Convert accumulated sums to matrix entries
        for (incurred_bucket, exposure_bucket), sum_value in bucket_sums.items():
            if abs(sum_value) > self.ZERO_THRESHOLD:
                entries.append(ExposureMatrixEntry(
                    incurred_date_bucket=incurred_bucket,
                    exposure_date_bucket=exposure_bucket,
                    sum=sum_value
                ))
        
        return entries
    
    def _get_active_incurred_buckets(self) -> List[date]:
        """Get incurred bucket dates that have non-zero values."""
        active_buckets = set(entry.incurred_date_bucket for entry in self.entries)
        return sorted([bucket for bucket in self._original_incurred_dates if bucket in active_buckets])
    
    def _get_active_exposure_buckets(self) -> List[date]:
        """Get exposure bucket dates that have non-zero values."""
        active_buckets = set(entry.exposure_date_bucket for entry in self.entries)
        return sorted([bucket for bucket in self._original_exposure_dates if bucket in active_buckets])
    
    def _get_bucket_date(self, target_date: date, bucket_dates: List[date]) -> Optional[date]:
        """
        Determine which bucket a date falls into.
        
        Args:
            target_date: The date to categorize
            bucket_dates: List of bucket boundary dates
            
        Returns:
            The bucket date that contains the target date, or None if no bucket contains it
        """
        if not bucket_dates:
            return None
            
        if self.to_end:
            # Find the first bucket end date that is >= target_date
            for bucket_date in bucket_dates:
                if target_date <= bucket_date:
                    return bucket_date
        else:
            # Find the last bucket start date that is <= target_date
            for bucket_date in reversed(bucket_dates):
                if target_date >= bucket_date:
                    return bucket_date
        
        return None
    
    def get_matrix_entries(self) -> List[ExposureMatrixEntry]:
        """Get all matrix entries."""
        return self.entries.copy()
    
    def get_exposure_vector(self) -> List[ExposureVectorEntry]:
        """Get exposure vector by summing across incurred dates for each exposure bucket."""
        exposure_sums = {}
        
        for entry in self.entries:
            bucket = entry.exposure_date_bucket
            exposure_sums[bucket] = exposure_sums.get(bucket, 0.0) + entry.sum
        
        vector = []
        for bucket_date in self.exposure_bucket_dates:
            if bucket_date in exposure_sums and abs(exposure_sums[bucket_date]) > self.ZERO_THRESHOLD:
                vector.append(ExposureVectorEntry(
                    date_bucket=bucket_date,
                    sum=exposure_sums[bucket_date]
                ))
        
        return vector
    
    def get_incurred_vector(self) -> List[ExposureVectorEntry]:
        """Get incurred vector by summing across exposure dates for each incurred bucket."""
        incurred_sums = {}
        
        for entry in self.entries:
            bucket = entry.incurred_date_bucket
            incurred_sums[bucket] = incurred_sums.get(bucket, 0.0) + entry.sum
        
        vector = []
        for bucket_date in self.incurred_bucket_dates:
            if bucket_date in incurred_sums and abs(incurred_sums[bucket_date]) > self.ZERO_THRESHOLD:
                vector.append(ExposureVectorEntry(
                    date_bucket=bucket_date,
                    sum=incurred_sums[bucket_date]
                ))
        
        return vector
    
    def get_total_sum(self) -> float:
        """Get the total sum of all matrix entries."""
        return sum(entry.sum for entry in self.entries)
    
    def get_matrix_value(self, incurred_bucket: date, exposure_bucket: date) -> float:
        """Get the value for a specific bucket combination."""
        for entry in self.entries:
            if (entry.incurred_date_bucket == incurred_bucket and 
                entry.exposure_date_bucket == exposure_bucket):
                return entry.sum
        return 0.0
    
    def format_matrix_table(self, precision: int = 2) -> str:
        """
        Format the matrix as a table string.
        
        Args:
            precision: Number of decimal places for formatting numbers
            
        Returns:
            Formatted string representation of the matrix
        """
        if not self.entries:
            return "Empty matrix"
        
        # Create header
        header = f"{self.EXPOSURE_HEADER:<{self.COLUMN_WIDTH}}"
        for exp_date in self.exposure_bucket_dates:
            header += f"{exp_date.strftime('%Y-%m-%d'):<{self.COLUMN_WIDTH}}"
        header += f"{'Total':<{self.COLUMN_WIDTH}}"
        
        lines = [header]
        lines.append("-" * len(header))
        
        # Create rows for each incurred bucket
        for inc_date in self.incurred_bucket_dates:
            row = f"{inc_date.strftime('%Y-%m-%d'):<{self.COLUMN_WIDTH}}"
            row_total = 0.0
            
            for exp_date in self.exposure_bucket_dates:
                value = self.get_matrix_value(inc_date, exp_date)
                row_total += value
                formatted_value = f"{value:.{precision}f}" if abs(value) > self.ZERO_THRESHOLD else "0"
                row += f"{formatted_value:<{self.COLUMN_WIDTH}}"
            
            formatted_total = f"{row_total:.{precision}f}" if abs(row_total) > self.ZERO_THRESHOLD else "0"
            row += f"{formatted_total:<{self.COLUMN_WIDTH}}"
            lines.append(row)
        
        # Add totals row
        totals_row = f"{'Total':<{self.COLUMN_WIDTH}}"
        grand_total = 0.0
        
        for exp_date in self.exposure_bucket_dates:
            col_total = sum(entry.sum for entry in self.entries 
                           if entry.exposure_date_bucket == exp_date)
            grand_total += col_total
            formatted_total = f"{col_total:.{precision}f}" if abs(col_total) > self.ZERO_THRESHOLD else "0"
            totals_row += f"{formatted_total:<{self.COLUMN_WIDTH}}"
        
        formatted_grand_total = f"{grand_total:.{precision}f}" if abs(grand_total) > self.ZERO_THRESHOLD else "0"
        totals_row += f"{formatted_grand_total:<{self.COLUMN_WIDTH}}"
        
        lines.append("-" * len(header))
        lines.append(totals_row)
        
        return "\n".join(lines)

    @staticmethod
    def _validate_constructor_inputs(factors, incurred_buckets, exposure_buckets):
        if factors is None:
            raise ValueError("factors cannot be None")
        if incurred_buckets is None:
            raise ValueError("incurred_bucket_dates cannot be None")
        if exposure_buckets is None:
            raise ValueError("exposure_bucket_dates cannot be None")
        if len(incurred_buckets) == 0:
            raise ValueError("incurred_bucket_dates cannot be empty")
        if len(exposure_buckets) == 0:
            raise ValueError("exposure_bucket_dates cannot be empty")
