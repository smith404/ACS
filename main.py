from datetime import date
from calendar_factory import CalendarFactory, TimeUnit
from pattern_factor import PatternFactor

def main():
    input_date = date(2022, 8, 2)
    factor = PatternFactor(
        time_unit=TimeUnit.QUARTER,
        up_front=10,
        up_front_duration=360,
        distributed=60,
        distributed_duration=360
    )

    print(factor.get_element_end_date(input_date))
    print(factor.get_element_end_duration(input_date))

    print(factor.get_duration_dates(input_date))
    print(factor.get_factor_dates(input_date))
if __name__ == "__main__":
    main()
