# Coverage Pattern Reader - Databricks Usage Guide

## Overview
The `CoveragePatternReader` is a PySpark-based tool designed for Databricks that reads coverage allocation data and constructs `Pattern` objects with QS (upfront) and Q (distributed) portions. It integrates with your existing `Pattern` and `PatternFactor` classes.

## Data Structure
The reader works with three CSV files in the `scratch/gold/` directory:

### 1. host_coverage_pattern_allocation.csv
Maps coverage IDs to pattern IDs with start dates:
```csv
coverage_id,writing_premium_pattern_id,pattern_start_date
100,1000,2023-01-01
101,1001,2023-01-01
102,1002,2023-01-01
```

### 2. host_pattern_factor.csv
Defines the factors for each pattern:
```csv
pattern_id,period_frequency,period,factor_value,duration
1000,Q,1,1.0,360
1001,QS,1,0.1284,360
1001,Q,1,0.2179,360
1001,Q,2,0.2179,360
1001,Q,3,0.2179,360
1001,Q,4,0.2179,360
```

### 3. host_pattern.csv
Pattern metadata:
```csv
pattern_id,pattern_name
1000,Simple 1 quarer all written
1001,Classic 4 quarters
1002,All Upfront
```

## Key Concepts

### Period Frequency Types
- **QS (Quarter Start)**: Upfront portion paid at the beginning
- **Q (Quarter)**: Distributed portion spread over quarterly periods
- **QE (Quarter Exposure)**: NEW - Derived factors from exposure vector with duration 0

### Pattern Types
1. **Writing Patterns**: Original patterns defined in CSV with QS/Q factors
2. **Loss Incurred Patterns**: NEW - Derived patterns created from exposure vectors with QE factors

### Pattern Examples
- **Pattern 1000**: Single Q factor (1.0) - All premium distributed in first quarter
- **Pattern 1001**: QS factor (0.1284) + 4 Q factors (0.2179 each) - 12.84% upfront + 87.16% distributed over 4 quarters
- **Loss Pattern LI_1001**: 8 QE factors derived from exposure vector - Each quarter's exposure as a QE factor

## Databricks Usage

### Basic Usage
```python
from coverage_pattern_reader import create_coverage_pattern_reader

# Create reader (uses existing spark session in Databricks)
reader = create_coverage_pattern_reader(spark, "scratch/gold")

# Get pattern for a coverage ID
pattern = reader.construct_pattern_object(100)
print(f"Pattern: {pattern.name}")
print(f"Elements: {len(pattern.elements)}")
```

### Simple Function for Quick Lookups
```python
from coverage_pattern_reader import get_pattern_for_coverage

# Direct pattern lookup
pattern = get_pattern_for_coverage(100)
upfront_total = sum(e.up_front for e in pattern.elements)
distributed_total = sum(e.distribution for e in pattern.elements)
```

### Pattern Summary with Details
```python
# Get detailed summary
summary = reader.get_pattern_summary(101)
print(f"Pattern Start Date: {summary['pattern_start_date']}")
print(f"Upfront Total: {summary['total_upfront_factor']:.4f}")
print(f"Distributed Total: {summary['total_distributed_factor']:.4f}")
print(f"Upfront Factors: {summary['upfront_factors']}")
print(f"Distributed Factors: {summary['distributed_factors']}")
```

### NEW - Generate Pattern Factors with Start Date
```python
# Generate factors using pattern start date
factors = reader.generate_pattern_factors(101)
print(f"Generated {len(factors)} factors")
for factor in factors[:3]:
    print(f"Incurred: {factor.incurred_date}, Exposed: {factor.exposed_date}, Value: {factor.value:.6f}")
```

### NEW - Calculate Exposure Matrix
```python
# Calculate exposure matrix with financial quarters
matrix = reader.calculate_exposure_matrix(101)
print(f"Matrix entries: {len(matrix.get_matrix_entries())}")
print(f"Total sum: {matrix.get_total_sum():.6f}")
print(matrix.format_matrix_table(precision=6))
```

### NEW - Get Exposure and Incurred Vectors
```python
# Get exposure vector (financial quarters)
exposure_vector = reader.get_exposure_vector(101)
for entry in exposure_vector:
    print(f"Quarter {entry.date_bucket}: {entry.sum:.6f}")

# Get incurred vector
incurred_vector = reader.get_incurred_vector(101)
for entry in incurred_vector:
    print(f"Quarter {entry.date_bucket}: {entry.sum:.6f}")
```

### NEW - Create Loss Incurred Pattern from Exposure Vector
```python
# Create loss incurred pattern with QE factors
loss_pattern = reader.create_loss_incurred_pattern(101)
print(f"Loss Pattern: {loss_pattern.name}")
print(f"Start Date: {loss_pattern.start_date}")
print(f"QE Factors: {len(loss_pattern.elements)}")

# Show QE factors
for element in loss_pattern.elements:
    print(f"QE: {element.exposure_date} → {element.distribution:.6f} (duration: {element.duration})")
```

### NEW - Get Loss Incurred Pattern Summary
```python
# Get loss pattern summary
loss_summary = reader.get_loss_incurred_pattern_summary(101)
print(f"Original Writing Pattern: {loss_summary['original_writing_pattern_id']}")
print(f"Loss Pattern: {loss_summary['loss_pattern_name']}")
print(f"QE Factors: {loss_summary['qe_factors_count']}")
print(f"Total Distribution: {loss_summary['total_exposure_distribution']:.6f}")
```

### NEW - Complete Analysis (Both Patterns)
```python
# Get analysis of both writing and loss incurred patterns
complete = reader.get_complete_pattern_analysis(101)
writing = complete['writing_pattern']
loss = complete['loss_incurred_pattern']

print(f"Writing: {writing['pattern_name']} → {writing['exposure_vector_count']} quarters")
print(f"Loss: {loss['loss_pattern_name']} → {loss['qe_factors_count']} QE factors")
```

### Batch Processing
```python
# Process multiple coverages efficiently
coverage_ids = [100, 101, 102]
results = reader.batch_process_coverages(coverage_ids)
for result in results:
    print(f"Coverage {result['coverage_id']}: {result['pattern_name']}")
```

## Key Methods

### CoveragePatternReader.construct_pattern_object(coverage_id)
- **Returns**: `Pattern` object with integrated `PatternFactor` elements and start date
- **Use Case**: When you need to work with the full Pattern object structure

### CoveragePatternReader.get_pattern_summary(coverage_id) 
- **Returns**: Dictionary with comprehensive pattern information including start date
- **Use Case**: When you need detailed analysis data including factor breakdowns

### NEW - CoveragePatternReader.generate_pattern_factors(coverage_id)
- **Returns**: List of `Factor` objects generated from the pattern using start date
- **Use Case**: When you need actual incurred/exposed date factors for calculations

### NEW - CoveragePatternReader.calculate_exposure_matrix(coverage_id)
- **Returns**: `ExposureMatrix` object with financial quarter buckets
- **Use Case**: When you need exposure analysis across financial quarters

### NEW - CoveragePatternReader.get_exposure_vector(coverage_id)
- **Returns**: List of `ExposureVectorEntry` objects for financial quarters
- **Use Case**: When you need exposure distribution by quarter

### NEW - CoveragePatternReader.get_incurred_vector(coverage_id)
- **Returns**: List of `ExposureVectorEntry` objects for incurred quarters
- **Use Case**: When you need incurred distribution by quarter

### NEW - CoveragePatternReader.create_loss_incurred_pattern(coverage_id)
- **Returns**: `Pattern` object with QE (Quarter Exposure) factors derived from exposure vector
- **Use Case**: When you need a loss incurred pattern based on exposure distribution

### NEW - CoveragePatternReader.get_loss_incurred_pattern_summary(coverage_id)
- **Returns**: Dictionary with loss incurred pattern details including QE factors
- **Use Case**: When you need summary information about the derived loss pattern

### NEW - CoveragePatternReader.get_complete_pattern_analysis(coverage_id)
- **Returns**: Dictionary with analysis of both writing pattern and derived loss incurred pattern
- **Use Case**: When you need comprehensive analysis of both patterns together

### CoveragePatternReader.batch_process_coverages(coverage_ids)
- **Returns**: List of pattern summaries
- **Use Case**: Processing multiple coverages efficiently

## Integration with Existing Classes

The reader creates `Pattern` objects using your existing classes:
- `Pattern`: Main container with `elements` list
- `PatternFactor`: Individual factors with `up_front` and `distribution` values
- `Type`: Enum for time periods (DAY, WEEK, MONTH, QUARTER, YEAR)

## Example Output

For coverage 101 (Pattern 1001 - "Classic 4 quarters" starting 2023-01-01):

### Pattern Summary
```python
{
    'coverage_id': 101,
    'pattern_id': 1001,
    'pattern_name': 'Classic 4 quarters',
    'pattern_start_date': '2023-01-01',
    'upfront_factors_count': 1,
    'distributed_factors_count': 4,
    'total_upfront_factor': 0.1284,
    'total_distributed_factor': 0.8716,
    'upfront_factors': [
        {'period': 1, 'factor_value': 0.1284, 'duration': 360, 'factor_type': 'QUARTER'}
    ],
    'distributed_factors': [
        {'period': 1, 'factor_value': 0.2179, 'duration': 360, 'factor_type': 'QUARTER'},
        {'period': 2, 'factor_value': 0.2179, 'duration': 360, 'factor_type': 'QUARTER'},
        {'period': 3, 'factor_value': 0.2179, 'duration': 360, 'factor_type': 'QUARTER'},
        {'period': 4, 'factor_value': 0.2179, 'duration': 360, 'factor_type': 'QUARTER'}
    ]
}
```

### NEW - Comprehensive Pattern Analysis
```python
{
    'coverage_id': 101,
    'pattern_id': 1001,
    'pattern_name': 'Classic 4 quarters',
    'pattern_start_date': '2023-01-01',
    'factors_count': 134233,
    'matrix_entries_count': 20,
    'matrix_total_sum': 1.000000,
    'exposure_vector_count': 8,
    'incurred_vector_count': 4,
    'date_ranges': {
        'min_incurred': '2023-01-01',
        'max_incurred': '2023-12-31',
        'min_exposed': '2023-01-01',
        'max_exposed': '2024-12-31'
    },
    'exposure_vector': [
        {'date_bucket': '2023-03-31', 'sum': 0.058525},
        {'date_bucket': '2023-06-30', 'sum': 0.113426},
        {'date_bucket': '2023-09-30', 'sum': 0.169446},
        {'date_bucket': '2023-12-31', 'sum': 0.224218},
        {'date_bucket': '2024-03-31', 'sum': 0.189396},
        {'date_bucket': '2024-06-30', 'sum': 0.135443},
        {'date_bucket': '2024-09-30', 'sum': 0.082159},
        {'date_bucket': '2024-12-31', 'sum': 0.027386}
    ],
    'incurred_vector': [
        {'date_bucket': '2023-03-31', 'sum': 0.346300},
        {'date_bucket': '2023-06-30', 'sum': 0.217900},
        {'date_bucket': '2023-09-30', 'sum': 0.217900},
        {'date_bucket': '2023-12-31', 'sum': 0.217900}
    ]
}
```

### NEW - Exposure Matrix Table
```
Exp x Inc   2023-03-31  2023-06-30  2023-09-30  2023-12-31  2024-03-31  2024-06-30  2024-09-30  2024-12-31  Total
------------------------------------------------------------------------------------------------------------------------
2023-03-31  0.058525    0.086338    0.087287    0.087287    0.026864    0           0           0           0.346300
2023-06-30  0           0.027089    0.054773    0.054773    0.054177    0.027089    0           0           0.217900
2023-09-30  0           0           0.027386    0.054773    0.054177    0.054177    0.027386    0           0.217900
2023-12-31  0           0           0           0.027386    0.054177    0.054177    0.054773    0.027386    0.217900
------------------------------------------------------------------------------------------------------------------------
Total       0.058525    0.113426    0.169446    0.224218    0.189396    0.135443    0.082159    0.027386    1.000000
```

## Error Handling

The reader gracefully handles missing data:
- Returns `None` for non-existent coverage IDs
- Returns error dictionaries with descriptive messages
- Validates data integrity using the Pattern class's `check_distribution()` method

## Performance Notes

- Uses PySpark DataFrames for efficient data processing
- Lazy loading of CSV files (loaded once, cached for subsequent calls)
- Optimized for Databricks cluster environments
- Batch processing available for multiple coverages
