import unittest
from datetime import date, timedelta
from calendar_factory import CalendarFactory, TimeUnit


class TestTimeUnit(unittest.TestCase):
    def test_time_unit_values(self):
        self.assertEqual(TimeUnit.DAY.label, "day")
        self.assertEqual(TimeUnit.DAY.duration, 1)
        self.assertEqual(TimeUnit.WEEK.label, "week")
        self.assertEqual(TimeUnit.WEEK.duration, 7)
        self.assertEqual(TimeUnit.MONTH.label, "month")
        self.assertEqual(TimeUnit.MONTH.duration, 30)
        self.assertEqual(TimeUnit.QUARTER.label, "quarter")
        self.assertEqual(TimeUnit.QUARTER.duration, 90)
        self.assertEqual(TimeUnit.YEAR.label, "year")
        self.assertEqual(TimeUnit.YEAR.duration, 360)


class TestCalendarFactory(unittest.TestCase):
    def setUp(self):
        self.test_date = date(2022, 11, 29)
        self.calendar_factory = CalendarFactory()

    def test_init(self):
        pass

    def test_get_time_unit_duration(self):
        self.assertEqual(self.calendar_factory.get_time_unit_duration(TimeUnit.DAY), 1)
        self.assertEqual(self.calendar_factory.get_time_unit_duration(TimeUnit.WEEK), 7)
        self.assertEqual(self.calendar_factory.get_time_unit_duration(TimeUnit.MONTH), 30)
        self.assertEqual(self.calendar_factory.get_time_unit_duration(TimeUnit.QUARTER), 90)
        self.assertEqual(self.calendar_factory.get_time_unit_duration(TimeUnit.YEAR), 360)

    def test_add_day(self):
        start_date = date(2022, 11, 29)
        result = self.calendar_factory.add_day(start_date)
        self.assertEqual(result, date(2022, 11, 30))

    def test_add_week(self):
        start_date = date(2022, 11, 29)
        result = self.calendar_factory._add_week(start_date)
        self.assertEqual(result, date(2022, 12, 6))

    def test_add_year_normal(self):
        start_date = date(2022, 11, 29)
        result = self.calendar_factory._add_year(start_date)
        self.assertEqual(result, date(2023, 11, 29))

    def test_add_year_leap_day(self):
        start_date = date(2020, 2, 29)  # Leap year
        result = self.calendar_factory._add_year(start_date)
        self.assertEqual(result, date(2021, 2, 28))  # Non-leap year fallback

    def test_add_month_normal(self):
        start_date = date(2022, 1, 15)
        result = self.calendar_factory._add_month(start_date, False, False, None)
        self.assertEqual(result, date(2022, 2, 15))

    def test_add_month_end_of_month(self):
        start_date = date(2022, 1, 31)
        result = self.calendar_factory._add_month(start_date, True, False, None)
        self.assertEqual(result, date(2022, 2, 28))

    def test_add_month_preserve_day(self):
        start_date = date(2022, 1, 30)
        result = self.calendar_factory._add_month(start_date, False, True, 30)
        self.assertEqual(result, date(2022, 2, 28))  # February doesn't have 30 days

    def test_add_month_year_boundary(self):
        start_date = date(2022, 12, 15)
        result = self.calendar_factory._add_month(start_date, False, False, None)
        self.assertEqual(result, date(2023, 1, 15))

    def test_add_quarter_normal(self):
        start_date = date(2022, 1, 15)
        result = self.calendar_factory._add_quarter(start_date, False, False, None)
        self.assertEqual(result, date(2022, 4, 15))

    def test_add_quarter_end_of_month(self):
        start_date = date(2022, 1, 31)
        result = self.calendar_factory._add_quarter(start_date, True, False, None)
        self.assertEqual(result, date(2022, 4, 30))

    def test_add_quarter_year_boundary(self):
        start_date = date(2022, 10, 15)
        result = self.calendar_factory._add_quarter(start_date, False, False, None)
        self.assertEqual(result, date(2023, 1, 15))

    def test_get_relative_dates_day(self):
        start_date = date(2022, 11, 29)
        dates = self.calendar_factory.get_relative_dates(start_date, TimeUnit.DAY, 3)
        expected = [date(2022, 11, 29), date(2022, 11, 30), date(2022, 12, 1)]
        self.assertEqual(dates, expected)

    def test_get_relative_dates_week(self):
        start_date = date(2022, 11, 29)
        dates = self.calendar_factory.get_relative_dates(start_date, TimeUnit.WEEK, 3)
        expected = [date(2022, 11, 29), date(2022, 12, 6), date(2022, 12, 13)]
        self.assertEqual(dates, expected)

    def test_get_relative_dates_month(self):
        start_date = date(2022, 1, 15)
        dates = self.calendar_factory.get_relative_dates(start_date, TimeUnit.MONTH, 3)
        expected = [date(2022, 1, 15), date(2022, 2, 15), date(2022, 3, 15)]
        self.assertEqual(dates, expected)

    def test_get_relative_dates_quarter(self):
        start_date = date(2022, 1, 15)
        dates = self.calendar_factory.get_relative_dates(start_date, TimeUnit.QUARTER, 3)
        expected = [date(2022, 1, 15), date(2022, 4, 15), date(2022, 7, 15)]
        self.assertEqual(dates, expected)

    def test_get_relative_dates_year(self):
        start_date = date(2022, 1, 15)
        dates = self.calendar_factory.get_relative_dates(start_date, TimeUnit.YEAR, 3)
        expected = [date(2022, 1, 15), date(2023, 1, 15), date(2024, 1, 15)]
        self.assertEqual(dates, expected)

    def test_get_quarter_end_dates(self):
        start_date = date(2022, 1, 15)
        dates = self.calendar_factory.get_quarter_end_dates(start_date, 3)
        expected = [date(2022, 3, 31), date(2022, 6, 30), date(2022, 9, 30)]
        self.assertEqual(dates, expected)

    def test_get_quarter_end_dates_already_at_end(self):
        start_date = date(2022, 3, 31)  # Last day of Q1
        dates = self.calendar_factory.get_quarter_end_dates(start_date, 2)
        expected = [date(2022, 6, 30), date(2022, 9, 30)]
        self.assertEqual(dates, expected)

    def test_get_quarter_start_dates(self):
        start_date = date(2022, 2, 15)
        dates = self.calendar_factory.get_quarter_start_dates(start_date, 3)
        expected = [date(2022, 1, 1), date(2022, 4, 1), date(2022, 7, 1)]
        self.assertEqual(dates, expected)

    def test_get_quarter_start_dates_year_boundary(self):
        start_date = date(2022, 11, 15)
        dates = self.calendar_factory.get_quarter_start_dates(start_date, 3)
        expected = [date(2022, 10, 1), date(2023, 1, 1), date(2023, 4, 1)]
        self.assertEqual(dates, expected)

    def test_last_day_of_month(self):
        self.assertEqual(self.calendar_factory._last_day_of_month(2022, 1), 31)
        self.assertEqual(self.calendar_factory._last_day_of_month(2022, 2), 28)
        self.assertEqual(self.calendar_factory._last_day_of_month(2020, 2), 29)  # Leap year
        self.assertEqual(self.calendar_factory._last_day_of_month(2022, 4), 30)
        self.assertEqual(self.calendar_factory._last_day_of_month(2022, 12), 31)

    def test_combine_and_sort_dates(self):
        dates1 = [date(2022, 1, 3), date(2022, 1, 1)]
        dates2 = [date(2022, 1, 2), date(2022, 1, 1)]  # Duplicate
        result = self.calendar_factory.combine_and_sort_dates(dates1, dates2)
        expected = [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)]
        self.assertEqual(result, expected)

    def test_add_duration_360_simple(self):
        input_date = date(2022, 1, 1)
        result = self.calendar_factory.add_duration_360(input_date, 392)
        # 392 = 1 year + 1 month + 2 days = 360 + 30 + 2
        expected = date(2023, 2, 3)
        self.assertEqual(result, expected)

    def test_add_duration_360_with_overflow(self):
        input_date = date(2022, 11, 1)
        result = self.calendar_factory.add_duration_360(input_date, 60)  # 2 months
        expected = date(2023, 1, 1)
        self.assertEqual(result, expected)

    def test_add_duration_360_invalid_day(self):
        input_date = date(2022, 1, 31)
        result = self.calendar_factory.add_duration_360(input_date, 30)  # 1 month
        # February doesn't have 31 days, should fallback to last day
        expected = date(2022, 2, 28)
        self.assertEqual(result, expected)

    def test_edge_case_leap_year(self):
        # Test month addition with leap year
        leap_date = date(2020, 2, 29)
        factory = CalendarFactory()
        dates = factory.get_relative_dates(leap_date, TimeUnit.MONTH, 2)
        # February 29 is end of month, so should go to end of March
        self.assertEqual(dates[0], date(2020, 2, 29))
        self.assertEqual(dates[1], date(2020, 3, 31))  # End of March, not March 29

    def test_edge_case_end_of_month_preservation(self):
        # Test end of month preservation
        end_of_month = date(2022, 1, 31)
        factory = CalendarFactory()
        dates = factory.get_relative_dates(end_of_month, TimeUnit.MONTH, 3)
        # Should go to end of each month
        self.assertEqual(dates[0], date(2022, 1, 31))
        self.assertEqual(dates[1], date(2022, 2, 28))  # February end
        self.assertEqual(dates[2], date(2022, 3, 31))  # March end

    def test_empty_buckets(self):
        start_date = date(2022, 1, 1)
        dates = self.calendar_factory.get_relative_dates(start_date, TimeUnit.DAY, 0)
        self.assertEqual(dates, [])

    def test_single_bucket(self):
        start_date = date(2022, 1, 1)
        dates = self.calendar_factory.get_relative_dates(start_date, TimeUnit.DAY, 1)
        self.assertEqual(dates, [date(2022, 1, 1)])

    def test_add_duration_360(self):
        """Test adding duration using 360-day calendar."""
        result = self.calendar_factory.add_duration_360(self.test_date, 392)
        self.assertIsInstance(result, date)
        # Test with known values
        expected_date = self.calendar_factory.add_duration_360(date(2022, 1, 1), 360)
        self.assertIsInstance(expected_date, date)

    def test_add_duration_360_zero_days(self):
        """Test adding zero duration."""
        result = self.calendar_factory.add_duration_360(self.test_date, 0)
        self.assertEqual(result, self.test_date)

if __name__ == '__main__':
    unittest.main()
