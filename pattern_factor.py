from datetime import date
from calendar_factory import CalendarFactory, TimeUnit


class PatternFactor:
    def __init__(self, time_unit, up_front, up_front_duration, distributed, distributed_duration):
        self.time_unit = time_unit
        self.up_front = up_front
        self.up_front_duration = 0 if up_front == 0 else up_front_duration
        self.distributed = distributed
        self.distributed_duration = 0 if distributed == 0 else distributed_duration

    def get_factor(self):
        return self.factor_value

    def get_element_end_date(self, start_date: date):
        calendar_factory = CalendarFactory()
        return calendar_factory.add_duration_360(start_date, self.time_unit.duration)

    def get_element_end_duration(self, start_date: date):
        calendar_factory = CalendarFactory()
        time_unit_duration = self.time_unit.duration
        total_duration = time_unit_duration + self.distributed_duration
        return calendar_factory.add_duration_360(start_date, max(self.up_front_duration, total_duration))

    def get_duration_dates(self, start_date: date):
        end_date = self.get_element_end_duration(start_date)
        return self.get_relative_dates(start_date, end_date)
    
    def get_factor_dates(self, start_date: date):
        end_date = self.get_element_end_date(start_date)
        return self.get_relative_dates(start_date, end_date)
    
    def get_relative_dates(self, start_date: date, end_date: date):
        calendar_factory = CalendarFactory()
        dates = calendar_factory.get_relative_dates_until(start_date, TimeUnit.MONTH, end_date)
        dates.extend(calendar_factory.get_month_end_dates_until(start_date, end_date, include_final=False))
        # Flatten, remove duplicates, and sort
        flat_dates = []
        for item in dates:
            if isinstance(item, list):
                flat_dates.extend(item)
            else:
                flat_dates.append(item)
        unique_sorted_dates = sorted(set(flat_dates))
        return unique_sorted_dates
