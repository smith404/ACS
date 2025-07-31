# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from datetime import date, timedelta
from enum import Enum


class TimeUnit(Enum):
    DAY = ("day", 1)
    WEEK = ("week", 7)
    MONTH = ("month", 30)
    QUARTER = ("quarter", 90)
    YEAR = ("year", 360)

    def __init__(self, label, duration):
        self.label = label
        self.duration = duration


class CalendarFactory:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CalendarFactory, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not CalendarFactory._initialized:
            CalendarFactory._initialized = True

    def get_relative_dates(self, start_date: date, time_unit: TimeUnit, buckets: int):
        dates = []
        current_date = start_date

        initial_last_day = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        initial_is_end_of_month = start_date.day == initial_last_day.day
        preserve_day = (start_date.day == 29 or start_date.day == 30) and not initial_is_end_of_month
        preserved_day = start_date.day if preserve_day else None

        for _ in range(buckets):
            dates.append(current_date)
            if time_unit == TimeUnit.DAY:
                current_date = self.add_day(current_date)
            elif time_unit == TimeUnit.WEEK:
                current_date = self._add_week(current_date)
            elif time_unit == TimeUnit.MONTH:
                current_date = self._add_month(current_date, initial_is_end_of_month, preserve_day, preserved_day)
            elif time_unit == TimeUnit.QUARTER:
                current_date = self._add_quarter(current_date, initial_is_end_of_month, preserve_day, preserved_day)
            elif time_unit == TimeUnit.YEAR:
                current_date = self._add_year(current_date)
        return dates

    def get_relative_dates_until(self, start_date: date, time_unit: TimeUnit, end_date: date):
        dates = []
        current_date = start_date

        initial_last_day = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        initial_is_end_of_month = start_date.day == initial_last_day.day
        preserve_day = (start_date.day == 29 or start_date.day == 30) and not initial_is_end_of_month
        preserved_day = start_date.day if preserve_day else None

        while current_date <= end_date:
            dates.append(current_date)
            if time_unit == TimeUnit.DAY:
                current_date = self.add_day(current_date)
            elif time_unit == TimeUnit.WEEK:
                current_date = self._add_week(current_date)
            elif time_unit == TimeUnit.MONTH:
                current_date = self._add_month(current_date, initial_is_end_of_month, preserve_day, preserved_day)
            elif time_unit == TimeUnit.QUARTER:
                current_date = self._add_quarter(current_date, initial_is_end_of_month, preserve_day, preserved_day)
            elif time_unit == TimeUnit.YEAR:
                current_date = self._add_year(current_date)
        return dates

    def add_day(self, current_date):
        return current_date + timedelta(days=1)

    def _add_week(self, current_date):
        return current_date + timedelta(weeks=1)

    def _add_month(self, current_date, initial_is_end_of_month, preserve_day, preserved_day):
        month = current_date.month + 1
        year = current_date.year + (month - 1) // 12
        month = (month - 1) % 12 + 1
        if initial_is_end_of_month:
            next_last_day = (date(year, month, 28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            return next_last_day
        else:
            day = preserved_day if preserve_day else current_date.day
            try:
                return current_date.replace(year=year, month=month, day=day)
            except ValueError:
                next_last_day = (date(year, month, 28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                return next_last_day

    def _add_quarter(self, current_date, initial_is_end_of_month, preserve_day, preserved_day):
        month = current_date.month + 3
        year = current_date.year + (month - 1) // 12
        month = (month - 1) % 12 + 1
        if initial_is_end_of_month:
            next_last_day = (date(year, month, 28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            return next_last_day
        else:
            day = preserved_day if preserve_day else current_date.day
            try:
                return current_date.replace(year=year, month=month, day=day)
            except ValueError:
                next_last_day = (date(year, month, 28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                return next_last_day

    def _add_year(self, current_date):
        try:
            return current_date.replace(year=current_date.year + 1)
        except ValueError:
            return current_date.replace(month=2, day=28, year=current_date.year + 1)

    def get_quarter_end_dates(self, start_date: date, buckets: int, include_final: bool = False):
        # Find the next quarter end after start_date
        month = ((start_date.month - 1) // 3 + 1) * 3
        year = start_date.year
        if start_date.month > month or (start_date.month == month and start_date.day == self._last_day_of_month(year, month)):
            # Move to next quarter
            month += 3
            if month > 12:
                month -= 12
                year += 1
        quarter_end = date(year, month, self._last_day_of_month(year, month))
        dates = []
        for _ in range(buckets):
            dates.append(quarter_end)
            # Move to next quarter end
            month += 3
            if month > 12:
                month -= 12
                year += 1
            quarter_end = date(year, month, self._last_day_of_month(year, month))
        if include_final:
            dates.append(quarter_end)
        return dates

    def get_quarter_end_dates_until(self, start_date: date, end_date: date, include_final: bool = False):
        month = ((start_date.month - 1) // 3 + 1) * 3
        year = start_date.year
        if start_date.month > month or (start_date.month == month and start_date.day == self._last_day_of_month(year, month)):
            month += 3
            if month > 12:
                month -= 12
                year += 1
        quarter_end = date(year, month, self._last_day_of_month(year, month))
        dates = []
        while quarter_end <= end_date:
            dates.append(quarter_end)
            month += 3
            if month > 12:
                month -= 12
                year += 1
            quarter_end = date(year, month, self._last_day_of_month(year, month))
        if include_final:
            dates.append(quarter_end)
        return dates

    def get_month_end_dates(self, start_date: date, buckets: int, include_final: bool = False):
        year = start_date.year
        month = start_date.month
        month_end = date(year, month, self._last_day_of_month(year, month))
        dates = []
        for _ in range(buckets):
            dates.append(month_end)
            month += 1
            if month > 12:
                month = 1
                year += 1
            month_end = date(year, month, self._last_day_of_month(year, month))
        if include_final:
            dates.append(month_end)
        return dates

    def get_month_end_dates_until(self, start_date: date, end_date: date, include_final: bool = False):
        year = start_date.year
        month = start_date.month
        month_end = date(year, month, self._last_day_of_month(year, month))
        dates = []
        while month_end <= end_date:
            dates.append(month_end)
            month += 1
            if month > 12:
                month = 1
                year += 1
            month_end = date(year, month, self._last_day_of_month(year, month))
        if include_final:
            dates.append(month_end)
        return dates

    def get_quarter_start_dates(self, start_date: date, buckets: int):
        # Find the start of the quarter equal to or before start_date
        quarter_month = ((start_date.month - 1) // 3) * 3 + 1
        year = start_date.year
        quarter_start = date(year, quarter_month, 1)
        dates = []
        for _ in range(buckets):
            dates.append(quarter_start)
            # Move to next quarter start
            quarter_month += 3
            if quarter_month > 12:
                quarter_month -= 12
                year += 1
            quarter_start = date(year, quarter_month, 1)
        return dates

    def get_quarter_start_dates_until(self, start_date: date, end_date: date):
        quarter_month = ((start_date.month - 1) // 3) * 3 + 1
        year = start_date.year
        quarter_start = date(year, quarter_month, 1)
        dates = []
        while quarter_start <= end_date:
            dates.append(quarter_start)
            quarter_month += 3
            if quarter_month > 12:
                quarter_month -= 12
                year += 1
            quarter_start = date(year, quarter_month, 1)
        return dates

    def get_month_start_dates(self, start_date: date, buckets: int):
        year = start_date.year
        month = start_date.month
        month_start = date(year, month, 1)
        dates = []
        for _ in range(buckets):
            dates.append(month_start)
            month += 1
            if month > 12:
                month = 1
                year += 1
            month_start = date(year, month, 1)
        return dates

    def get_month_start_dates_until(self, start_date: date, end_date: date):
        year = start_date.year
        month = start_date.month
        month_start = date(year, month, 1)
        dates = []
        while month_start <= end_date:
            dates.append(month_start)
            month += 1
            if month > 12:
                month = 1
                year += 1
            month_start = date(year, month, 1)
        return dates

    def _last_day_of_month(self, year, month):
        # Helper to get last day of a month
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        return (next_month - timedelta(days=1)).day

    def combine_and_sort_dates(self, dates1, dates2):
        combined = list(set(dates1) | set(dates2))
        combined.sort()
        return combined

    def add_duration_360(self, input_date: date, duration: int):
        years = duration // 360
        months = (duration % 360) // 30
        days = duration % 30
        year = input_date.year + years
        month = input_date.month + months
        # Adjust year and month for overflow
        while month > 12:
            year += 1
            month -= 12
        day = input_date.day
        # Try to construct the new date, fallback to last valid day if needed
        try:
            result_date = date(year, month, day) + timedelta(days=days)
        except ValueError:
            # If day is not valid (e.g. Feb 30), fallback to last day of month
            last_day = self._last_day_of_month(year, month)
            result_date = date(year, month, last_day) + timedelta(days=days)
        return result_date

    def get_time_unit_duration(self, time_unit: TimeUnit):
        return time_unit.duration

    def get_factor_date_range(self, factors, use_incurred=True):
        if not factors:
            return None, None
        
        dates = []
        for factor in factors:
            if use_incurred:
                dates.append(factor.incurred_date)
            else:
                dates.append(factor.exposed_date)
        
        return min(dates), max(dates)

    def get_financial_quarter_end_dates(self, factors, use_incurred=True):
        if not factors:
            return []
        
        min_date, max_date = self.get_factor_date_range(factors, use_incurred)
        if min_date is None or max_date is None:
            return []
        
        # Get first quarter start of min_date year and last quarter end of max_date year
        start_of_first_year = date(min_date.year, 1, 1)
        end_of_last_year = date(max_date.year, 12, 31)
        
        return self.get_quarter_end_dates_until(start_of_first_year, end_of_last_year)

    def get_development_quarter_end_dates(self, factors, start_date: date, use_incurred=True):
        if not factors:
            return []
        
        min_date, max_date = self.get_factor_date_range(factors, use_incurred)
        if min_date is None or max_date is None:
            return []
        
        # Get first quarter start of min_date year and last quarter end of max_date year
        end_of_last_year = date(max_date.year, 12, 31)
        
        return self.get_relative_dates_until(start_date, TimeUnit.QUARTER, end_of_last_year)
