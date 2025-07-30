# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from pattern import Pattern
from pattern_evaluator import PatternEvaluator
from pattern_factor import PatternFactor, Factor, Type
from datetime import date
from calendar_factory import CalendarFactory, TimeUnit
from exposure_matrix import ExposureMatrix


def main():
    pattern = Pattern()
    pattern.set_identifier("Test Pattern")
    pattern.add_element(PatternFactor(Type.QUARTER, 0.1284, 0.2179, 360, 360))
    pattern.add_element(PatternFactor(Type.QUARTER, 0, 0.2179, 0, 360))
    pattern.add_element(PatternFactor(Type.QUARTER, 0, 0.2179, 0, 360))
    pattern.add_element(PatternFactor(Type.QUARTER, 0, 0.2179, 0, 360))

    pattern.display()
    print("Distribution check:", pattern.check_distribution())
    
    # Generate factors and print count
    start_date = date(2023, 8, 2)
    factors = pattern.get_factors(start_date, use_calendar=True)
    print(f"Generated {len(factors)} factors")
    
    # Get min and max dates using CalendarFactory
    calendar_factory = CalendarFactory()
    min_incurred, max_incurred = calendar_factory.get_factor_date_range(factors, use_incurred=True)
    min_earned, max_earned = calendar_factory.get_factor_date_range(factors, use_incurred=False)
    
    print(f"Incurred date range: {min_incurred} to {max_incurred}")
    print(f"Earned date range: {min_earned} to {max_earned}")
    
    # Get quarter end dates from start of start_date year to end of max_incurred year
    if max_incurred:
        incurred_quarter_end = date(max_incurred.year, 12, 31)
        earned_quarter_end = date(max_earned.year, 12, 31)

        incurred_financial_quarter_dates = calendar_factory.get_financial_quarter_end_dates(factors, use_incurred=True)
        earned_financial_quarter_dates = calendar_factory.get_financial_quarter_end_dates(factors, use_incurred=False)

        incurred_development_quarter_dates = calendar_factory.get_relative_dates_until(start_date, TimeUnit.QUARTER, incurred_quarter_end)
        earned_development_quarter_dates = calendar_factory.get_relative_dates_until(start_date, TimeUnit.QUARTER, earned_quarter_end)

        combined_incurred_quarter_dates = calendar_factory.combine_and_sort_dates(
            incurred_financial_quarter_dates, incurred_development_quarter_dates
        )

        combined_earned_quarter_dates = calendar_factory.combine_and_sort_dates(
            earned_financial_quarter_dates, earned_development_quarter_dates
        )

        print(f"Incurred financial quarter end dates: {incurred_financial_quarter_dates}")
        print(f"Earned financial quarter end dates: {earned_financial_quarter_dates}")
        print(f"Incurred development quarter end dates: {incurred_development_quarter_dates}")
        print(f"Earned development quarter end dates: {earned_development_quarter_dates}")
        print(f"Combined incurred quarter end dates: {combined_incurred_quarter_dates}")
        print(f"Combined earned quarter end dates: {combined_earned_quarter_dates}")
    
    # Test ExposureMatrix
    print("\n=== Testing ExposureMatrix ===")
    exposure_matrix = ExposureMatrix(
        factors=factors,
        start_date=start_date,
        incurred_bucket_end_dates=incurred_financial_quarter_dates,
        exposure_bucket_end_dates=earned_financial_quarter_dates,
        to_end=True
    )
    
    # Generate matrix table
    matrix_table = exposure_matrix.generate_exposure_matrix_table(precision=4, csv=False)
    print("Exposure Matrix Table:")
    print(matrix_table)
    
    # Generate exposure vector (by exposure date)
    exposure_vector = exposure_matrix.generate_exposure_vector(is_exposure=True)
    print("\nExposure Vector (by exposure date):")
    for entry in exposure_vector:
        print(f"{entry.date_bucket}: {entry.sum:.6f}")
    
    # Generate incurred vector (by incurred date)
    incurred_vector = exposure_matrix.generate_exposure_vector(is_exposure=False)
    print("\nIncurred Vector (by incurred date):")
    for entry in incurred_vector:
        print(f"{entry.date_bucket}: {entry.sum:.6f}")
    
    # Test date criteria summation
    test_date = date(2024, 6, 30)
    sum_before = exposure_matrix.get_sum_by_date_criteria(test_date, "<=", "<=")
    print(f"\nSum where incurred <= {test_date} and exposure <= {test_date}: {sum_before:.6f}")


if __name__ == "__main__":
    main()
