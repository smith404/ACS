# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from datetime import date, timedelta
from pattern_slice import PatternSlice

class CalendarFactor:
    def __init__(self, input_date: date):
        self.input_date = input_date

    def print_start_end_dates(self, pattern_slice):
        start_date = self.input_date + timedelta(days=pattern_slice.startOffset)
        end_date = start_date + timedelta(days=pattern_slice.duration)
        print(f"Start Date: {start_date}, End Date: {end_date}")

    def get_dates_based_on_pattern(self, pattern_slice):
        dates = []
        start_date = self.input_date + timedelta(days=pattern_slice.startOffset)
        for i in range(pattern_slice.duration):
            dates.append(start_date + timedelta(days=i))
        return dates

def main():
    input_date = date(2023, 2, 27)
    pattern_slice = PatternSlice(duration=90, startOffset=0, distribution=0.2, startDistribution=0.1, durationOffset=30, developmentPeriods=2)
    calendar_factor = CalendarFactor(input_date)

    dates = calendar_factor.get_dates_based_on_pattern(pattern_slice)
    print(dates)

if __name__ == "__main__":
    main()
