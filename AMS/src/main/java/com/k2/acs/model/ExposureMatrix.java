package com.k2.acs.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class ExposureMatrix implements DateCriteriaSummable {

    public enum ExposureType {
        INCURRED,
        REPORTED,
        DUE,
        SETTLED
    }

    @Data
    @AllArgsConstructor
    public static class ExposureMatrixEntry {
        private final LocalDate incurredDateBucket;
        private final LocalDate exposureDateBucket;
        private final double sum;
        private ExposureType exposureType;
    }

    @Data
    @AllArgsConstructor
    public static class ExposureVectorEntry {
        private final LocalDate dateBucket;
        private final double sum;
        private ExposureType exposureType;
    }

    private static double roundToPrecision(double value, int precision) {
        return BigDecimal.valueOf(value)
                .setScale(precision, RoundingMode.HALF_UP)
                .doubleValue();
    }

    public static List<LocalDate> getBucketEndDates(LocalDate startDate, int endYear, PatternElement.Type frequency) {
        LocalDate endDate = LocalDate.of(endYear, 12, 31);
        return getBucketEndDates(startDate, endDate, frequency);
    }

    public static List<LocalDate> getBucketEndDates(LocalDate startDate, LocalDate endDate, PatternElement.Type frequency) {
        List<LocalDate> endDates = new ArrayList<>();
        LocalDate currentDate = startDate;
        while (!currentDate.isAfter(endDate)) {
            switch (frequency) {
                case DAY -> currentDate = currentDate.plusDays(1);
                case WEEK -> currentDate = currentDate.plusWeeks(1);
                case MONTH -> currentDate = currentDate.plusMonths(1);
                case QUARTER -> currentDate = currentDate.plusMonths(3);
                case YEAR -> currentDate = currentDate.plusYears(1);
                default -> throw new IllegalArgumentException("Unsupported frequency type: " + frequency);
            }
            LocalDate previousDate = currentDate.minusDays(1);
            if (!previousDate.isAfter(endDate) || previousDate.isEqual(endDate)) {
                endDates.add(previousDate);
            }
        }
        return endDates;
    }

    public static List<LocalDate> getEndDatesBetween(int startYear, int endYear, PatternElement.Type frequency) {
        List<LocalDate> endDates = new ArrayList<>();
        LocalDate currentDate = LocalDate.of(startYear, 1, 1);
        LocalDate endDate = LocalDate.of(endYear, 12, 31);

        while (!currentDate.isAfter(endDate)) {
            switch (frequency) {
                case DAY -> currentDate = currentDate.plusDays(1);
                case WEEK -> currentDate = currentDate.plusWeeks(1);
                case MONTH -> currentDate = currentDate.plusMonths(1);
                case QUARTER -> currentDate = currentDate.plusMonths(3);
                case YEAR -> currentDate = currentDate.plusYears(1);
                default -> throw new IllegalArgumentException("Unsupported frequency type: " + frequency);
            }
            LocalDate previousDate = currentDate.minusDays(1);
            if (!previousDate.isAfter(endDate) || previousDate.isEqual(endDate)) {
                endDates.add(previousDate);
            }
        }

        return endDates;
    }

    private List<ExposureMatrixEntry> matrixEntries;

    public ExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredBucketEndDates, List<LocalDate> exposureBucketEndDates) {
        this(factors, startDate, incurredBucketEndDates, exposureBucketEndDates, true);
    }

    public ExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredBucketEndDates, List<LocalDate> exposureBucketEndDates, boolean toEnd) {
        this.matrixEntries = generateExposureMatrix(factors, startDate, incurredBucketEndDates, exposureBucketEndDates, toEnd);
    }

    private List<ExposureMatrixEntry> generateExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredBucketEndDates, List<LocalDate> exposureBucketEndDates, boolean toEnd) {
        matrixEntries = new ArrayList<>();

        for (int i = 0; i < incurredBucketEndDates.size(); i++) {
            LocalDate incurredStart = getStartDate(startDate, incurredBucketEndDates, i);
            LocalDate incurredEnd = incurredBucketEndDates.get(i);
            if (incurredStart.isAfter(incurredEnd)) {
                // Skip if the start date is after the end date
                continue;
            }

            for (int j = 0; j < exposureBucketEndDates.size(); j++) {
                LocalDate exposureStart = getStartDate(startDate, exposureBucketEndDates, j);
                LocalDate exposureEnd = exposureBucketEndDates.get(j);
                if (exposureStart.isAfter(exposureEnd)) {
                    // Skip if the start date is after the end date
                    continue;
                }
                double sum = calculateSum(factors, incurredStart, incurredEnd, exposureStart, exposureEnd);
                addMatrixEntry(toEnd, incurredStart, incurredEnd, exposureStart, exposureEnd, sum, ExposureType.INCURRED);
            }
        }

        return matrixEntries;
    }

    private LocalDate getStartDate(LocalDate startDate, List<LocalDate> buckets, int index) {
        return index == 0 ? startDate : buckets.get(index - 1).plusDays(1);
    }

    private double calculateSum(List<Factor> factors, LocalDate incurredStart, LocalDate incurredEnd, LocalDate exposureStart, LocalDate exposureEnd) {
        return factors.stream()
                .filter(factor -> isWithinRange(factor.getIncurredDate(), incurredStart, incurredEnd))
                .filter(factor -> isWithinRange(factor.getExposureDate(), exposureStart, exposureEnd))
                .mapToDouble(Factor::getValue)
                .sum();
    }

    private boolean isWithinRange(LocalDate date, LocalDate start, LocalDate end) {
        return !date.isBefore(start) && !date.isAfter(end);
    }

    private void addMatrixEntry(boolean toEnd, LocalDate incurredStart, LocalDate incurredEnd, LocalDate exposureStart, LocalDate exposureEnd, double sum, ExposureType exposureType) {
        if (sum == 0) {
            return;
        }
        matrixEntries.add(new ExposureMatrixEntry(
                toEnd ? incurredEnd : incurredStart,
                toEnd ? exposureEnd : exposureStart,
                sum,
                exposureType));
    }


    public String generateExposureMatrixTable(int precision) {
        String formatString = "%" + (10 + precision) + "s";
        String formatDouble = "%" + (10 + precision) + "." + precision + "f";
        List<LocalDate> exposureBucketEndDates = matrixEntries.stream()
                .map(ExposureMatrixEntry::getExposureDateBucket)
                .distinct()
                .sorted()
                .toList();
        List<LocalDate> incurredBucketEndDates = matrixEntries.stream()
                .map(ExposureMatrixEntry::getIncurredDateBucket)
                .distinct()
                .sorted()
                .toList();

        StringBuilder table = new StringBuilder();
        table.append(String.format("%10s", "Exp x Inc"));
        for (LocalDate exposureDate : exposureBucketEndDates) {
            table.append(String.format(formatString, exposureDate));
        }
        table.append("\n");
        for (LocalDate incurredDate : incurredBucketEndDates) {
            table.append(String.format("%10s", incurredDate));
            for (LocalDate exposureDate : exposureBucketEndDates) {
                double sum = matrixEntries.stream()
                        .filter(entry -> entry.getIncurredDateBucket().equals(incurredDate) &&
                                entry.getExposureDateBucket().equals(exposureDate))
                        .mapToDouble(ExposureMatrixEntry::getSum)
                        .sum();
                table.append(String.format(formatDouble, roundToPrecision(sum, precision)));
            }
            table.append("\n");
        }
        return table.toString();
    }


    public List<ExposureVectorEntry> generateExposureVector() {
        return generateExposureVector(true);
    }

    public List<ExposureVectorEntry> generateExposureVector(boolean isExposure) {
        List<ExposureVectorEntry> vector = new ArrayList<>();
        if (matrixEntries == null || matrixEntries.isEmpty()) {
            return vector;
        }

        if (isExposure) {
            // Aggregate by exposureDateBucket
            matrixEntries.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                    ExposureMatrixEntry::getExposureDateBucket,
                    java.util.stream.Collectors.summingDouble(ExposureMatrixEntry::getSum)
                ))
                .forEach((date, sum) -> vector.add(
                    new ExposureVectorEntry(date, sum, ExposureType.INCURRED)
                ));
        } else {
            // Aggregate by incurredDateBucket
            matrixEntries.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                    ExposureMatrixEntry::getIncurredDateBucket,
                    java.util.stream.Collectors.summingDouble(ExposureMatrixEntry::getSum)
                ))
                .forEach((date, sum) -> vector.add(
                    new ExposureVectorEntry(date, sum, ExposureType.INCURRED)
                ));
        }

        // Sort by date
        vector.sort(java.util.Comparator.comparing(ExposureVectorEntry::getDateBucket));
        return vector;
    }

    public List<LocalDate> getExposureBuckets() {
        return matrixEntries.stream()
                .map(ExposureMatrixEntry::getExposureDateBucket)
                .distinct()
                .sorted()
                .toList();
    }

    public List<LocalDate> getIncurredBuckets() {
        return matrixEntries.stream()
                .map(ExposureMatrixEntry::getIncurredDateBucket)
                .distinct()
                .sorted()
                .toList();
    }

    @Override
    public double getSumByDateCriteria(LocalDate date,
                                       String incurredDateComparison,
                                       String exposureDateComparison) {
        return matrixEntries.stream()
                .filter(entry -> compareDates(entry.getIncurredDateBucket(), date, incurredDateComparison) &&
                        compareDates(entry.getExposureDateBucket(), date, exposureDateComparison))
                .mapToDouble(ExposureMatrixEntry::getSum)
                .sum();
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
}
