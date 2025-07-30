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
    COLUMN_WIDTH = 10
    ZERO_THRESHOLD = 1e-10
    
    def __init__(self, factors: List[Factor], start_date: date, 
                 incurred_bucket_end_dates: List[date], 
                 exposure_bucket_end_dates: List[date], 
                 to_end: bool = True):
        """
        Initialize the ExposureMatrix.
        
        Args:
            factors: List of Factor objects
            start_date: Starting date for bucket generation
            incurred_bucket_end_dates: List of incurred bucket end dates
            exposure_bucket_end_dates: List of exposure bucket end dates
            to_end: If True, use end dates for buckets; if False, use start dates
        """
        self._validate_constructor_inputs(factors, start_date, incurred_bucket_end_dates, exposure_bucket_end_dates)
        self.matrix_entries = self._generate_exposure_matrix(
            factors, start_date, incurred_bucket_end_dates, exposure_bucket_end_dates, to_end
        )
    
    @staticmethod
    def get_bucket_end_dates(start_date: date, end_year: int, frequency: Type) -> List[date]:
        """
        Generate bucket end dates from a start date to the end of a specified year.
        
        Args:
            start_date: Starting date for bucket generation
            end_year: Year to end bucket generation
            frequency: Frequency of buckets (DAY, WEEK, MONTH, QUARTER, YEAR)
            
        Returns:
            List of bucket end dates
        """
        end_date = date(end_year, 12, 31)
        return ExposureMatrix.get_bucket_end_dates_range(start_date, end_date, frequency)
    
    @staticmethod
    def get_bucket_end_dates_range(start_date: date, end_date: date, frequency: Type) -> List[date]:
        """
        Generate bucket end dates between two dates with specified frequency.
        
        Args:
            start_date: Starting date for bucket generation
            end_date: Ending date for bucket generation
            frequency: Frequency of buckets
            
        Returns:
            List of bucket end dates
        """
        if start_date is None or end_date is None or frequency is None:
            raise ValueError("start_date, end_date, and frequency cannot be None")
        if start_date > end_date:
            raise ValueError("start_date cannot be after end_date")
        
        end_dates = []
        current_date = start_date
        
        while current_date <= end_date:
            current_date = ExposureMatrix._advance_date_by_frequency(current_date, frequency)
            bucket_end_date = current_date - timedelta(days=1)
            
            if bucket_end_date <= end_date:
                end_dates.append(bucket_end_date)
        
        return end_dates
    
    @staticmethod
    def get_end_dates_between(start_year: int, end_year: int, frequency: Type) -> List[date]:
        """
        Generate end dates between two years with specified frequency, starting from January 1st.
        
        Args:
            start_year: Starting year
            end_year: Ending year
            frequency: Frequency of buckets
            
        Returns:
            List of bucket end dates
        """
        if start_year > end_year:
            raise ValueError("start_year cannot be after end_year")
        if frequency is None:
            raise ValueError("frequency cannot be None")
        
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)
        return ExposureMatrix.get_bucket_end_dates_range(start_date, end_date, frequency)
    
    def generate_exposure_matrix_table(self, precision: int, csv: bool = False) -> str:
        """
        Generate the exposure matrix as a table or CSV.
        
        Args:
            precision: Number of decimal places
            csv: If True, outputs as CSV
            
        Returns:
            String table or CSV representation
        """
        if precision < 0:
            raise ValueError("precision must be non-negative")
        
        exposure_buckets = self.get_exposure_buckets()
        incurred_buckets = self.get_incurred_buckets()
        
        if csv:
            return self._generate_csv_table(precision, exposure_buckets, incurred_buckets)
        else:
            return self._generate_formatted_table(precision, exposure_buckets, incurred_buckets)
    
    def generate_exposure_vector(self, is_exposure: bool = True) -> List[ExposureVectorEntry]:
        """
        Generate an exposure vector by aggregating matrix entries.
        
        Args:
            is_exposure: If True, aggregates by exposure date; if False, by incurred date
            
        Returns:
            List of vector entries
        """
        if not self.matrix_entries:
            return []
        
        if is_exposure:
            return self._aggregate_by_exposure_date()
        else:
            return self._aggregate_by_incurred_date()
    
    def get_exposure_buckets(self) -> List[date]:
        """Get all unique exposure bucket dates from the matrix."""
        return sorted(list(set(entry.exposure_date_bucket for entry in self.matrix_entries)))
    
    def get_incurred_buckets(self) -> List[date]:
        """Get all unique incurred bucket dates from the matrix."""
        return sorted(list(set(entry.incurred_date_bucket for entry in self.matrix_entries)))
    
    def get_sum_by_date_criteria(self, target_date: date, incurred_comparison: str, exposure_comparison: str) -> float:
        """
        Get sum of matrix entries matching date criteria.
        
        Args:
            target_date: Date to compare against
            incurred_comparison: Comparison operator for incurred date ('<', '<=', '>', '>=')
            exposure_comparison: Comparison operator for exposure date
            
        Returns:
            Sum of matching entries
        """
        self._validate_date_criteria_inputs(target_date, incurred_comparison, exposure_comparison)
        
        total = 0.0
        for entry in self.matrix_entries:
            if (self._compare_dates(entry.incurred_date_bucket, target_date, incurred_comparison) and
                self._compare_dates(entry.exposure_date_bucket, target_date, exposure_comparison)):
                total += entry.sum
        
        return total
    
    # Private helper methods
    
    @staticmethod
    def _validate_constructor_inputs(factors, start_date, incurred_buckets, exposure_buckets):
        if factors is None:
            raise ValueError("factors cannot be None")
        if start_date is None:
            raise ValueError("start_date cannot be None")
        if incurred_buckets is None:
            raise ValueError("incurred_bucket_end_dates cannot be None")
        if exposure_buckets is None:
            raise ValueError("exposure_bucket_end_dates cannot be None")
    
    @staticmethod
    def _advance_date_by_frequency(target_date: date, frequency: Type) -> date:
        """Advance date by the specified frequency."""
        if frequency == Type.DAY:
            return target_date + timedelta(days=1)
        elif frequency == Type.WEEK:
            return target_date + timedelta(weeks=1)
        elif frequency == Type.MONTH:
            if target_date.month == 12:
                return target_date.replace(year=target_date.year + 1, month=1)
            else:
                return target_date.replace(month=target_date.month + 1)
        elif frequency == Type.QUARTER:
            month = target_date.month + 3
            year = target_date.year
            if month > 12:
                month -= 12
                year += 1
            return target_date.replace(year=year, month=month)
        elif frequency == Type.YEAR:
            return target_date.replace(year=target_date.year + 1)
        else:
            raise ValueError(f"Unknown frequency: {frequency}")
    
    def _generate_exposure_matrix(self, factors: List[Factor], start_date: date,
                                incurred_buckets: List[date], exposure_buckets: List[date], to_end: bool):
        """Generate the exposure matrix entries."""
        entries = []
        
        for i, incurred_end in enumerate(incurred_buckets):
            incurred_range = self._create_date_range(start_date, incurred_buckets, i)
            if not incurred_range.is_valid():
                continue
            
            for j, exposure_end in enumerate(exposure_buckets):
                exposure_range = self._create_date_range(start_date, exposure_buckets, j)
                if not exposure_range.is_valid():
                    continue
                
                total_sum = self._calculate_factor_sum(factors, incurred_range, exposure_range)
                if abs(total_sum) > self.ZERO_THRESHOLD:
                    incurred_date = incurred_range.end if to_end else incurred_range.start
                    exposure_date = exposure_range.end if to_end else exposure_range.start
                    entries.append(ExposureMatrixEntry(incurred_date, exposure_date, total_sum))
        
        return entries
    
    def _create_date_range(self, start_date: date, buckets: List[date], index: int) -> DateRange:
        """Create a date range for the given bucket index."""
        range_start = start_date if index == 0 else buckets[index - 1] + timedelta(days=1)
        range_end = buckets[index]
        return DateRange(range_start, range_end)
    
    def _calculate_factor_sum(self, factors: List[Factor], incurred_range: DateRange, exposure_range: DateRange) -> float:
        """Calculate the sum of factors within the given date ranges."""
        total = 0.0
        for factor in factors:
            if (incurred_range.contains(factor.incurred_date) and 
                exposure_range.contains(factor.earned_date)):
                total += factor.value
        return total
    
    def _generate_csv_table(self, precision: int, exposure_buckets: List[date], incurred_buckets: List[date]) -> str:
        """Generate CSV format table."""
        lines = []
        
        # Header row
        header = [self.EXPOSURE_HEADER] + [str(date_bucket) for date_bucket in exposure_buckets]
        lines.append(",".join(header))
        
        # Data rows
        for incurred_date in incurred_buckets:
            row = [str(incurred_date)]
            for exposure_date in exposure_buckets:
                value = self._get_matrix_value(incurred_date, exposure_date)
                formatted_value = f"{value:.{precision}f}"
                row.append(formatted_value)
            lines.append(",".join(row))
        
        return "\n".join(lines)
    
    def _generate_formatted_table(self, precision: int, exposure_buckets: List[date], incurred_buckets: List[date]) -> str:
        """Generate formatted table."""
        lines = []
        column_width = self.COLUMN_WIDTH + precision
        
        # Header row
        header_parts = [f"{self.EXPOSURE_HEADER:>{self.COLUMN_WIDTH}}"]
        for date_bucket in exposure_buckets:
            header_parts.append(f"{date_bucket:>{column_width}}")
        lines.append("".join(header_parts))
        
        # Data rows
        for incurred_date in incurred_buckets:
            row_parts = [f"{incurred_date:>{self.COLUMN_WIDTH}}"]
            for exposure_date in exposure_buckets:
                value = self._get_matrix_value(incurred_date, exposure_date)
                formatted_value = f"{value:>{column_width}.{precision}f}"
                row_parts.append(formatted_value)
            lines.append("".join(row_parts))
        
        return "\n".join(lines)
    
    def _get_matrix_value(self, incurred_date: date, exposure_date: date) -> float:
        """Get the matrix value for the given incurred and exposure dates."""
        total = 0.0
        for entry in self.matrix_entries:
            if (entry.incurred_date_bucket == incurred_date and 
                entry.exposure_date_bucket == exposure_date):
                total += entry.sum
        return total
    
    def _aggregate_by_exposure_date(self) -> List[ExposureVectorEntry]:
        """Aggregate matrix entries by exposure date."""
        aggregation = {}
        for entry in self.matrix_entries:
            if entry.exposure_date_bucket not in aggregation:
                aggregation[entry.exposure_date_bucket] = 0.0
            aggregation[entry.exposure_date_bucket] += entry.sum
        
        return [ExposureVectorEntry(date_bucket, total) 
                for date_bucket, total in sorted(aggregation.items())]
    
    def _aggregate_by_incurred_date(self) -> List[ExposureVectorEntry]:
        """Aggregate matrix entries by incurred date."""
        aggregation = {}
        for entry in self.matrix_entries:
            if entry.incurred_date_bucket not in aggregation:
                aggregation[entry.incurred_date_bucket] = 0.0
            aggregation[entry.incurred_date_bucket] += entry.sum
        
        return [ExposureVectorEntry(date_bucket, total) 
                for date_bucket, total in sorted(aggregation.items())]
    
    def _validate_date_criteria_inputs(self, target_date: date, incurred_comparison: str, exposure_comparison: str):
        """Validate inputs for date criteria matching."""
        if target_date is None:
            raise ValueError("target_date cannot be None")
        self._validate_comparison_operator(incurred_comparison, "incurred date comparison")
        self._validate_comparison_operator(exposure_comparison, "exposure date comparison")
    
    def _validate_comparison_operator(self, operator: str, context: str):
        """Validate comparison operator."""
        valid_operators = ["<", "<=", ">", ">="]
        if operator not in valid_operators:
            raise ValueError(f"Invalid comparison operator for {context}: {operator}")
    
    def _compare_dates(self, bucket_date: date, target_date: date, comparison: str) -> bool:
        """Compare two dates using the specified comparison operator."""
        if comparison == "<":
            return bucket_date < target_date
        elif comparison == "<=":
            return bucket_date <= target_date
        elif comparison == ">":
            return bucket_date > target_date
        elif comparison == ">=":
            return bucket_date >= target_date
        else:
            raise ValueError(f"Invalid comparison operator: {comparison}")
