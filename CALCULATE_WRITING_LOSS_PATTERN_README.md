# Calculate Writing Loss Pattern Script

## Overview

This script calculates the writing loss pattern (loss incurred pattern) for a given coverage ID. It demonstrates the complete workflow from finding the writing premium pattern to generating QE (Quarter Exposure) factors.

## Usage

```bash
python calculate_writing_loss_pattern.py <coverage_id>
```

### Examples

```bash
# Simple pattern (all distributed)
python calculate_writing_loss_pattern.py 100

# Complex pattern (QS + 4Q)
python calculate_writing_loss_pattern.py 101

# All upfront pattern
python calculate_writing_loss_pattern.py 102
```

## What the Script Does

### Step 1: Writing Premium Pattern Lookup
- Finds the `writing_premium_pattern_id` for the given coverage ID
- Retrieves pattern metadata (name, start date, elements)
- Shows upfront vs distributed factor breakdown

### Step 2: Exposure Vector Calculation
- Calculates quarterly exposure vector from the writing pattern
- Uses financial quarter-end dates
- Validates total exposure sums to 1.000000

### Step 3: Writing Loss Pattern Creation
- Creates loss incurred pattern with QE factors
- Each QE factor has duration 0 (instant exposure)
- Maintains 1:1 mapping with exposure vector

### Step 4: Factor Table Display
- Shows detailed comparison between exposure vector and QE factors
- Validates perfect 1:1 mapping
- Confirms mathematical accuracy

## Sample Output

```
================================================================================
WRITING LOSS PATTERN CALCULATION FOR COVERAGE 101
================================================================================

📋 STEP 1: WRITING PREMIUM PATTERN LOOKUP
==================================================
✅ Writing Premium Pattern Found:
   Coverage ID: 101
   Writing Pattern ID: 1001
   Pattern Name: Classic 4 quarters
   Pattern Start Date: 2023-01-01
   Total Elements: 4
   Upfront Factor: 0.128400
   Distributed Factor: 0.871600

🎯 STEP 2: EXPOSURE VECTOR CALCULATION
==================================================
✅ Exposure Vector Generated:
   Total Quarters: 8
   Date Range: 2023-03-31 → 2024-12-31
   Total Exposure: 1.000000

🔄 STEP 3: WRITING LOSS PATTERN CREATION
==================================================
✅ Writing Loss Pattern Created:
   Loss Pattern Name: Loss incurred pattern for writing pattern 1001
   Loss Pattern ID: LI_1001
   Loss Start Date: 2023-03-31
   QE Factors Count: 8

📊 STEP 4: WRITING LOSS PATTERN FACTOR TABLE
==================================================

Index  Quarter End  Exposure     QE Factor    Duration Type   Match
----------------------------------------------------------------------
1      2023-03-31   0.058525     0.058525     0        QE     ✅
2      2023-06-30   0.113426     0.113426     0        QE     ✅
3      2023-09-30   0.169446     0.169446     0        QE     ✅
4      2023-12-31   0.224218     0.224218     0        QE     ✅
5      2024-03-31   0.189396     0.189396     0        QE     ✅
6      2024-06-30   0.135443     0.135443     0        QE     ✅
7      2024-09-30   0.082159     0.082159     0        QE     ✅
8      2024-12-31   0.027386     0.027386     0        QE     ✅
----------------------------------------------------------------------
TOTAL               1.000000     1.000000                     ✅

✅ VALIDATION SUMMARY
==================================================
   Exposure Vector Total: 1.000000
   QE Factors Total: 1.000000
   1:1 Mapping: ✅ Valid
   Sum Validation: ✅ Valid

🎉 Writing Loss Pattern Successfully Calculated!
```

## Technical Details

### QE Factors
- **Type**: Quarter Exposure (QE)
- **Duration**: Always 0 (instant exposure)
- **Value**: Directly mapped from exposure vector entries
- **Date**: Financial quarter-end dates

### Validation
- All exposure vectors sum to 1.000000
- All QE factor distributions sum to 1.000000
- Perfect 1:1 mapping between exposure vector and QE factors
- Date consistency across transformations

### Error Handling
- Invalid coverage IDs return clear error messages
- Duration validation ensures all factors have duration >= 1
- Mathematical validation checks for proper sum totals

## Dependencies
- PySpark 3.3.4
- Coverage Pattern Reader module
- Financial CalendarFactory for quarter calculations

## Related Files
- `coverage_pattern_reader.py`: Core pattern processing engine
- `scratch/gold/host_coverage_pattern_allocation.csv`: Coverage to pattern mapping
- `scratch/gold/host_pattern_factor.csv`: Pattern factor definitions
