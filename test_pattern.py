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
from pattern_factor import PatternFactor, Type
from datetime import date, timedelta
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
    start_date = date(2024, 8, 2)
    previous_date = start_date - timedelta(days=1)
    factors = pattern.get_factors(start_date, use_calendar=True)
    print(f"Generated {len(factors)} factors")
    
    # Get min and max dates using CalendarFactory
    calendar_factory = CalendarFactory()
    min_incurred, max_incurred = calendar_factory.get_factor_date_range(factors, use_incurred=True)
    min_exposed, max_exposed = calendar_factory.get_factor_date_range(factors, use_incurred=False)
    
    print(f"Incurred date range: {min_incurred} to {max_incurred}")
    print(f"exposed date range: {min_exposed} to {max_exposed}")
    
    # Get quarter end dates from start of start_date year to end of max_incurred year
    if max_incurred:
        incurred_quarter_end = date(max_incurred.year, 12, 31)
        exposed_quarter_end = date(max_exposed.year, 12, 31)

        incurred_financial_quarter_dates = calendar_factory.get_financial_quarter_end_dates(factors, use_incurred=True)
        exposed_financial_quarter_dates = calendar_factory.get_financial_quarter_end_dates(factors, use_incurred=False)

        # Use the previous date to get end dates of development periods
        incurred_development_quarter_dates = calendar_factory.get_relative_dates_until(previous_date, TimeUnit.QUARTER, incurred_quarter_end)
        exposed_development_quarter_dates = calendar_factory.get_relative_dates_until(previous_date, TimeUnit.QUARTER, exposed_quarter_end)

        combined_incurred_quarter_dates = calendar_factory.combine_and_sort_dates(
            incurred_financial_quarter_dates, incurred_development_quarter_dates
        )

        combined_exposed_quarter_dates = calendar_factory.combine_and_sort_dates(
            exposed_financial_quarter_dates, exposed_development_quarter_dates
        )

        print(f"Incurred financial quarter end dates: {incurred_financial_quarter_dates}")
        print(f"Exposed financial quarter end dates: {exposed_financial_quarter_dates}")
        print(f"Incurred development quarter end dates: {incurred_development_quarter_dates}")
        print(f"Exposed development quarter end dates: {exposed_development_quarter_dates}")
        print(f"Combined incurred quarter end dates: {combined_incurred_quarter_dates}")
        print(f"Combined exposed quarter end dates: {combined_exposed_quarter_dates}")
        
        # Test ExposureMatrix
        print("\n" + "="*50)
        print("TESTING EXPOSURE MATRIX")
        print("="*50)
        
        # Create exposure matrix
        matrix = ExposureMatrix(
            factors=factors,
            incurred_bucket_dates=incurred_development_quarter_dates,
            exposure_bucket_dates=exposed_financial_quarter_dates,
            to_end=True
        )
        
        # Display matrix statistics
        print(f"Matrix entries count: {len(matrix.get_matrix_entries())}")
        print(f"Total sum: {matrix.get_total_sum():.6f}")
        
        # Display matrix table
        print("\nMatrix Table:")
        print(matrix.format_matrix_table(precision=9))
        
        # Display exposure vector
        exposure_vector = matrix.get_exposure_vector()
        print(f"\nExposure Vector ({len(exposure_vector)} entries):")
        for entry in exposure_vector:
            print(f"  {entry.date_bucket}: {entry.sum:.6f}")
        
        # Display incurred vector
        incurred_vector = matrix.get_incurred_vector()
        print(f"\nIncurred Vector ({len(incurred_vector)} entries):")
        for entry in incurred_vector:
            print(f"  {entry.date_bucket}: {entry.sum:.6f}")
        
        # Test specific matrix value lookup
        if combined_incurred_quarter_dates and combined_exposed_quarter_dates:
            test_inc = combined_incurred_quarter_dates[0]
            test_exp = combined_exposed_quarter_dates[0]
            test_value = matrix.get_matrix_value(test_inc, test_exp)
            print(f"\nTest lookup - Incurred {test_inc}, Exposed {test_exp}: {test_value:.6f}")


if __name__ == "__main__":
    main()
