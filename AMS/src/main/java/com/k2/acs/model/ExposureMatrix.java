package com.k2.acs.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents an exposure matrix that aggregates factors into date buckets for incurred and exposure dates.
 * Provides functionality to generate matrix tables and vector summaries with configurable precision.
 */
public class ExposureMatrix implements DateCriteriaSummable {

    private static final String EXPOSURE_HEADER = "Exp x Inc";
    private static final int COLUMN_WIDTH = 10;
    private static final double ZERO_THRESHOLD = 1e-10;

    public record ExposureMatrixEntry(LocalDate incurredDateBucket, LocalDate exposureDateBucket, double sum) {
    }

    public record ExposureVectorEntry(LocalDate dateBucket, double sum) {
    }

    private final List<ExposureMatrixEntry> matrixEntries;

    public ExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredBucketEndDates, List<LocalDate> exposureBucketEndDates) {
        this(factors, startDate, incurredBucketEndDates, exposureBucketEndDates, true);
    }

    public ExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredBucketEndDates, List<LocalDate> exposureBucketEndDates, boolean toEnd) {
        validateConstructorInputs(factors, startDate, incurredBucketEndDates, exposureBucketEndDates);
        this.matrixEntries = generateExposureMatrix(factors, startDate, incurredBucketEndDates, exposureBucketEndDates, toEnd);
    }

    /**
     * Rounds a value to the specified precision using HALF_UP rounding.
     * @param value The value to round
     * @param precision Number of decimal places
     * @return Rounded value
     */
    private static double roundToPrecision(double value, int precision) {
        return BigDecimal.valueOf(value)
                .setScale(precision, RoundingMode.HALF_UP)
                .doubleValue();
    }

    /**
     * Generates bucket end dates from a start date to the end of a specified year.
     * @param startDate Starting date for bucket generation
     * @param endYear Year to end bucket generation
     * @param frequency Frequency of buckets (DAY, WEEK, MONTH, QUARTER, YEAR)
     * @return List of bucket end dates
     */
    public static List<LocalDate> getBucketEndDates(LocalDate startDate, int endYear, PatternFactor.Type frequency) {
        validateBucketInputs(startDate, frequency);
        LocalDate endDate = LocalDate.of(endYear, 12, 31);
        return getBucketEndDates(startDate, endDate, frequency);
    }

    /**
     * Generates bucket end dates between two dates with specified frequency.
     * @param startDate Starting date for bucket generation
     * @param endDate Ending date for bucket generation
     * @param frequency Frequency of buckets (DAY, WEEK, MONTH, QUARTER, YEAR)
     * @return List of bucket end dates
     */
    public static List<LocalDate> getBucketEndDates(LocalDate startDate, LocalDate endDate, PatternFactor.Type frequency) {
        validateDateRangeInputs(startDate, endDate, frequency);
        
        List<LocalDate> endDates = new ArrayList<>();
        LocalDate currentDate = startDate;
        
        while (!currentDate.isAfter(endDate)) {
            currentDate = advanceDateByFrequency(currentDate, frequency);
            LocalDate bucketEndDate = currentDate.minusDays(1);
            
            if (isBucketEndDateValid(bucketEndDate, endDate)) {
                endDates.add(bucketEndDate);
            }
        }
        
        return endDates;
    }

    /**
     * Generates end dates between two years with specified frequency, starting from January 1st.
     * @param startYear Starting year
     * @param endYear Ending year
     * @param frequency Frequency of buckets (DAY, WEEK, MONTH, QUARTER, YEAR)
     * @return List of bucket end dates
     */
    public static List<LocalDate> getEndDatesBetween(int startYear, int endYear, PatternFactor.Type frequency) {
        validateYearRange(startYear, endYear);
        if (frequency == null) {
            throw new IllegalArgumentException("Frequency cannot be null");
        }
        
        LocalDate startDate = LocalDate.of(startYear, 1, 1);
        LocalDate endDate = LocalDate.of(endYear, 12, 31);
        return getBucketEndDates(startDate, endDate, frequency);
    }

    /**
     * Generates the exposure matrix as a formatted table.
     * @param precision Number of decimal places
     * @return String table representation
     */
    public String generateExposureMatrixTable(int precision) {
        return generateExposureMatrixTable(precision, false);
    }

    /**
     * Generates the exposure matrix as a table or CSV.
     * @param precision Number of decimal places
     * @param csv If true, outputs as CSV
     * @return String table or CSV representation
     */
    public String generateExposureMatrixTable(int precision, boolean csv) {
        validatePrecision(precision);
        
        MatrixDimensions dimensions = extractMatrixDimensions();
        
        return csv ? 
            generateCsvTable(precision, dimensions) : 
            generateFormattedTable(precision, dimensions);
    }

    /**
     * Generates an exposure vector by aggregating matrix entries.
     * @return List of exposure vector entries aggregated by exposure date
     */
    public List<ExposureVectorEntry> generateExposureVector() {
        return generateExposureVector(true);
    }

    /**
     * Generates an exposure vector by aggregating matrix entries.
     * @param isExposure If true, aggregates by exposure date; if false, by incurred date
     * @return List of vector entries
     */
    public List<ExposureVectorEntry> generateExposureVector(boolean isExposure) {
        if (matrixEntries == null || matrixEntries.isEmpty()) {
            return new ArrayList<>();
        }

        return isExposure ? aggregateByExposureDate() : aggregateByIncurredDate();
    }

    /**
     * Gets all unique exposure bucket dates from the matrix.
     * @return Sorted list of exposure bucket dates
     */
    public List<LocalDate> getExposureBuckets() {
        return matrixEntries.stream()
                .map(ExposureMatrixEntry::exposureDateBucket)
                .distinct()
                .sorted()
                .toList();
    }

    /**
     * Gets all unique incurred bucket dates from the matrix.
     * @return Sorted list of incurred bucket dates
     */
    public List<LocalDate> getIncurredBuckets() {
        return matrixEntries.stream()
                .map(ExposureMatrixEntry::incurredDateBucket)
                .distinct()
                .sorted()
                .toList();
    }

    @Override
    public double getSumByDateCriteria(LocalDate date, String incurredDateComparison, String exposureDateComparison) {
        validateDateCriteriaInputs(date, incurredDateComparison, exposureDateComparison);
        
        return matrixEntries.stream()
                .filter(entry -> matchesDateCriteria(entry, date, incurredDateComparison, exposureDateComparison))
                .mapToDouble(ExposureMatrixEntry::sum)
                .sum();
    }

    // Private helper methods
    private static void validateConstructorInputs(List<Factor> factors, LocalDate startDate, 
                                                List<LocalDate> incurredBuckets, List<LocalDate> exposureBuckets) {
        if (factors == null) {
            throw new IllegalArgumentException("Factors list cannot be null");
        }
        if (startDate == null) {
            throw new IllegalArgumentException("Start date cannot be null");
        }
        if (incurredBuckets == null) {
            throw new IllegalArgumentException("Incurred bucket end dates cannot be null");
        }
        if (exposureBuckets == null) {
            throw new IllegalArgumentException("Exposure bucket end dates cannot be null");
        }
    }

    private static void validateBucketInputs(LocalDate startDate, PatternFactor.Type frequency) {
        if (startDate == null) {
            throw new IllegalArgumentException("Start date cannot be null");
        }
        if (frequency == null) {
            throw new IllegalArgumentException("Frequency cannot be null");
        }
    }

    private static void validateDateRangeInputs(LocalDate startDate, LocalDate endDate, PatternFactor.Type frequency) {
        validateBucketInputs(startDate, frequency);
        if (endDate == null) {
            throw new IllegalArgumentException("End date cannot be null");
        }
        if (startDate.isAfter(endDate)) {
            throw new IllegalArgumentException("Start date cannot be after end date");
        }
    }

    private static void validateYearRange(int startYear, int endYear) {
        if (startYear > endYear) {
            throw new IllegalArgumentException("Start year cannot be after end year");
        }
    }

    private static void validatePrecision(int precision) {
        if (precision < 0) {
            throw new IllegalArgumentException("Precision must be non-negative");
        }
    }

    private static LocalDate advanceDateByFrequency(LocalDate date, PatternFactor.Type frequency) {
        return switch (frequency) {
            case DAY -> date.plusDays(1);
            case WEEK -> date.plusWeeks(1);
            case MONTH -> date.plusMonths(1);
            case QUARTER -> date.plusMonths(3);
            case YEAR -> date.plusYears(1);
        };
    }

    private static boolean isBucketEndDateValid(LocalDate bucketEndDate, LocalDate endDate) {
        return !bucketEndDate.isAfter(endDate);
    }

    private List<ExposureMatrixEntry> generateExposureMatrix(List<Factor> factors, LocalDate startDate, 
                                                           List<LocalDate> incurredBuckets, List<LocalDate> exposureBuckets, boolean toEnd) {
        List<ExposureMatrixEntry> entries = new ArrayList<>();

        for (int i = 0; i < incurredBuckets.size(); i++) {
            DateRange incurredRange = createDateRange(startDate, incurredBuckets, i);
            if (!incurredRange.isValid()) continue;

            for (int j = 0; j < exposureBuckets.size(); j++) {
                DateRange exposureRange = createDateRange(startDate, exposureBuckets, j);
                if (!exposureRange.isValid()) continue;

                double sum = calculateFactorSum(factors, incurredRange, exposureRange);
                addMatrixEntryIfSignificant(entries, incurredRange, exposureRange, sum, toEnd);
            }
        }

        return entries;
    }

    private DateRange createDateRange(LocalDate startDate, List<LocalDate> buckets, int index) {
        LocalDate rangeStart = index == 0 ? startDate : buckets.get(index - 1).plusDays(1);
        LocalDate rangeEnd = buckets.get(index);
        return new DateRange(rangeStart, rangeEnd);
    }

    private double calculateFactorSum(List<Factor> factors, DateRange incurredRange, DateRange exposureRange) {
        return factors.stream()
                .filter(factor -> incurredRange.contains(factor.getIncurredDate()))
                .filter(factor -> exposureRange.contains(factor.getExposureDate()))
                .mapToDouble(Factor::getValue)
                .sum();
    }

    private void addMatrixEntryIfSignificant(List<ExposureMatrixEntry> entries, DateRange incurredRange, 
                                           DateRange exposureRange, double sum, boolean toEnd) {
        if (Math.abs(sum) > ZERO_THRESHOLD) {
            LocalDate incurredDate = toEnd ? incurredRange.end : incurredRange.start;
            LocalDate exposureDate = toEnd ? exposureRange.end : exposureRange.start;
            
            entries.add(new ExposureMatrixEntry(incurredDate, exposureDate, sum));
        }
    }

    private MatrixDimensions extractMatrixDimensions() {
        List<LocalDate> exposureBuckets = getExposureBuckets();
        List<LocalDate> incurredBuckets = getIncurredBuckets();
        return new MatrixDimensions(exposureBuckets, incurredBuckets);
    }

    private String generateCsvTable(int precision, MatrixDimensions dimensions) {
        StringBuilder table = new StringBuilder();
        
        // Header row
        table.append(EXPOSURE_HEADER);
        dimensions.exposureBuckets.forEach(date -> table.append(",").append(date));
        table.append("\n");
        
        // Data rows
        for (LocalDate incurredDate : dimensions.incurredBuckets) {
            table.append(incurredDate);
            for (LocalDate exposureDate : dimensions.exposureBuckets) {
                double sum = getMatrixValue(incurredDate, exposureDate);
                table.append(",").append(String.format("%." + precision + "f", roundToPrecision(sum, precision)));
            }
            table.append("\n");
        }
        
        return table.toString();
    }

    private String generateFormattedTable(int precision, MatrixDimensions dimensions) {
        StringBuilder table = new StringBuilder();
        String columnFormat = "%" + (COLUMN_WIDTH + precision) + "s";
        String doubleFormat = "%" + (COLUMN_WIDTH + precision) + "." + precision + "f";
        
        // Header row
        table.append(String.format("%" + COLUMN_WIDTH + "s", EXPOSURE_HEADER));
        dimensions.exposureBuckets.forEach(date -> table.append(String.format(columnFormat, date)));
        table.append("\n");
        
        // Data rows
        for (LocalDate incurredDate : dimensions.incurredBuckets) {
            table.append(String.format("%" + COLUMN_WIDTH + "s", incurredDate));
            for (LocalDate exposureDate : dimensions.exposureBuckets) {
                double sum = getMatrixValue(incurredDate, exposureDate);
                table.append(String.format(doubleFormat, roundToPrecision(sum, precision)));
            }
            table.append("\n");
        }
        
        return table.toString();
    }

    private double getMatrixValue(LocalDate incurredDate, LocalDate exposureDate) {
        return matrixEntries.stream()
                .filter(entry -> entry.incurredDateBucket().equals(incurredDate) &&
                               entry.exposureDateBucket().equals(exposureDate))
                .mapToDouble(ExposureMatrixEntry::sum)
                .sum();
    }

    private List<ExposureVectorEntry> aggregateByExposureDate() {
        return matrixEntries.stream()
                .collect(Collectors.groupingBy(
                    ExposureMatrixEntry::exposureDateBucket,
                    Collectors.summingDouble(ExposureMatrixEntry::sum)
                ))
                .entrySet().stream()
                .map(entry -> new ExposureVectorEntry(entry.getKey(), entry.getValue()))
                .sorted(java.util.Comparator.comparing(ExposureVectorEntry::dateBucket))
                .toList();
    }

    private List<ExposureVectorEntry> aggregateByIncurredDate() {
        return matrixEntries.stream()
                .collect(Collectors.groupingBy(
                    ExposureMatrixEntry::incurredDateBucket,
                    Collectors.summingDouble(ExposureMatrixEntry::sum)
                ))
                .entrySet().stream()
                .map(entry -> new ExposureVectorEntry(entry.getKey(), entry.getValue()))
                .sorted(java.util.Comparator.comparing(ExposureVectorEntry::dateBucket))
                .toList();
    }

    private void validateDateCriteriaInputs(LocalDate date, String incurredComparison, String exposureComparison) {
        if (date == null) {
            throw new IllegalArgumentException("Date cannot be null");
        }
        validateComparisonOperator(incurredComparison, "incurred date comparison");
        validateComparisonOperator(exposureComparison, "exposure date comparison");
    }

    private void validateComparisonOperator(String operator, String context) {
        if (operator == null || (!operator.equals("<") && !operator.equals("<=") && 
                                !operator.equals(">") && !operator.equals(">="))) {
            throw new IllegalArgumentException("Invalid comparison operator for " + context + ": " + operator);
        }
    }

    private boolean matchesDateCriteria(ExposureMatrixEntry entry, LocalDate date, 
                                      String incurredComparison, String exposureComparison) {
        return compareDates(entry.incurredDateBucket(), date, incurredComparison) &&
               compareDates(entry.exposureDateBucket(), date, exposureComparison);
    }

    private boolean compareDates(LocalDate bucketDate, LocalDate targetDate, String comparison) {
        return switch (comparison) {
            case "<" -> bucketDate.isBefore(targetDate);
            case "<=" -> !bucketDate.isAfter(targetDate);
            case ">" -> bucketDate.isAfter(targetDate);
            case ">=" -> !bucketDate.isBefore(targetDate);
            default -> throw new IllegalArgumentException("Invalid comparison operator: " + comparison);
        };
    }

    // Helper classes for better organization

    private record DateRange(LocalDate start, LocalDate end) {

        boolean isValid() {
                return !start.isAfter(end);
            }

            boolean contains(LocalDate date) {
                return !date.isBefore(start) && !date.isAfter(end);
            }
        }

    private record MatrixDimensions(List<LocalDate> exposureBuckets, List<LocalDate> incurredBuckets) {
    }
}
