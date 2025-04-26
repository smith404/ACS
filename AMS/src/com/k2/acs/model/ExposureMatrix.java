package com.k2.acs.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

public class ExposureMatrix {

    public enum ExposureType {
        INCURRED,
        REPORTED,
        DUE,
        SETTLED
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
            if (!currentDate.isAfter(endDate)) {
                endDates.add(currentDate.minusDays(1));
            }
        }

        return endDates;
    }

    public static List<LocalDate> getStartDatesBetween(int startYear, int endYear, PatternElement.Type frequency) {
        List<LocalDate> startDates = new ArrayList<>();
        LocalDate currentDate = LocalDate.of(startYear, 1, 1);
        LocalDate endDate = LocalDate.of(endYear, 12, 31);
        
        while (!currentDate.isAfter(endDate)) {
            startDates.add(currentDate);
            switch (frequency) {
                case DAY -> currentDate = currentDate.plusDays(1);
                case WEEK -> currentDate = currentDate.plusWeeks(1);
                case MONTH -> currentDate = currentDate.plusMonths(1);
                case QUARTER -> currentDate = currentDate.plusMonths(3);
                case YEAR -> currentDate = currentDate.plusYears(1);
                default -> throw new IllegalArgumentException("Unsupported frequency type: " + frequency);
            }
        }

        return startDates;
    }

    @Data
    public static class ExposureMatrixEntry {
        private final LocalDate incurredDateBucket;
        private final LocalDate exposureDateBucket;
        private final double sum;
        private ExposureType exposureType;

        public ExposureMatrixEntry(LocalDate incurredDateBucket, LocalDate exposureDateBucket, double sum) {
            this.incurredDateBucket = incurredDateBucket;
            this.exposureDateBucket = exposureDateBucket;
            this.sum = sum;
            this.exposureType = ExposureType.INCURRED;
        }
    }

    private final int precision;
    private final List<ExposureMatrixEntry> entries;

    public ExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredDateBuckets, List<LocalDate> exposureDateBuckets, int precision) {
        this.precision = precision;
        this.entries = generateExposureMatrix(factors, startDate, incurredDateBuckets, exposureDateBuckets, false);
    }

    public ExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredDateBuckets, List<LocalDate> exposureDateBuckets, int precision, boolean toEnd) {
        this.precision = precision;
        this.entries = generateExposureMatrix(factors, startDate, incurredDateBuckets, exposureDateBuckets, toEnd);
    }

    private List<ExposureMatrixEntry> generateExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredDateBuckets, List<LocalDate> exposureDateBuckets, boolean toEnd) {
        List<ExposureMatrixEntry> matrix = new ArrayList<>();

        for (int i = 0; i < incurredDateBuckets.size(); i++) {
            LocalDate incurredStart = getStartDate(startDate, incurredDateBuckets, i);
            LocalDate incurredEnd = incurredDateBuckets.get(i);

            for (int j = 0; j < exposureDateBuckets.size(); j++) {
                LocalDate exposureStart = getStartDate(startDate, exposureDateBuckets, j);
                LocalDate exposureEnd = exposureDateBuckets.get(j);

                double sum = calculateSum(factors, incurredStart, incurredEnd, exposureStart, exposureEnd);

                addMatrixEntry(matrix, toEnd, incurredStart, incurredEnd, exposureStart, exposureEnd, sum);
            }
        }

        return matrix;
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

    private void addMatrixEntry(List<ExposureMatrixEntry> matrix, boolean toEnd, LocalDate incurredStart, LocalDate incurredEnd, LocalDate exposureStart, LocalDate exposureEnd, double sum) {
        if (sum == 0) {
            return;
        }
        matrix.add(new ExposureMatrixEntry(
            toEnd ? incurredEnd : incurredStart,
            toEnd ? exposureEnd : exposureStart,
            roundToPrecision(sum)));
    }


    private double roundToPrecision(double value) {
        return BigDecimal.valueOf(value)
                         .setScale(precision, RoundingMode.HALF_UP)
                         .doubleValue();
    }

    public String generateExposureMatrixTable() {
        List<LocalDate> exposureDateBuckets = entries.stream()
                                                     .map(ExposureMatrixEntry::getExposureDateBucket)
                                                     .distinct()
                                                     .sorted()
                                                     .toList();
        List<LocalDate> incurredDateBuckets = entries.stream()
                                                     .map(ExposureMatrixEntry::getIncurredDateBucket)
                                                     .distinct()
                                                     .sorted()
                                                     .toList();

        StringBuilder table = new StringBuilder();
        table.append("Exp x Inc");
        for (LocalDate exposureDate : exposureDateBuckets) {
            table.append(",").append(exposureDate);
        }
        table.append("\n");

        // Populate the table rows
        for (LocalDate incurredDate : incurredDateBuckets) {
            table.append(incurredDate);
            for (LocalDate exposureDate : exposureDateBuckets) {
                double sum = entries.stream()
                                    .filter(entry -> entry.getIncurredDateBucket().equals(incurredDate) &&
                                                     entry.getExposureDateBucket().equals(exposureDate))
                                    .mapToDouble(ExposureMatrixEntry::getSum)
                                    .sum();
                table.append(",").append(roundToPrecision(sum));
            }
            table.append("\n");
        }

        return table.toString();
    }

    public String summarizeExposureMatrix() {
        // Extract unique buckets for x-axis and y-axis
        List<LocalDate> exposureDateBuckets = entries.stream()
                                                     .map(ExposureMatrixEntry::getExposureDateBucket)
                                                     .distinct()
                                                     .sorted()
                                                     .toList();
        List<LocalDate> incurredDateBuckets = entries.stream()
                                                     .map(ExposureMatrixEntry::getIncurredDateBucket)
                                                     .distinct()
                                                     .sorted()
                                                     .toList();

        Map<LocalDate, Double> columnSums = new HashMap<>();
        Map<LocalDate, Double> rowSums = new HashMap<>();
        double totalSum = 0;

        for (LocalDate incurredDate : incurredDateBuckets) {
            double rowSum = 0;
            for (LocalDate exposureDate : exposureDateBuckets) {
                double value = entries.stream()
                                      .filter(entry -> entry.getIncurredDateBucket().equals(incurredDate) &&
                                                       entry.getExposureDateBucket().equals(exposureDate))
                                      .mapToDouble(ExposureMatrixEntry::getSum)
                                      .sum();
                rowSum += value;
                columnSums.put(exposureDate, columnSums.getOrDefault(exposureDate, 0.0) + value);
            }
            rowSums.put(incurredDate, rowSum);
            totalSum += rowSum;
        }

        StringBuilder summary = new StringBuilder();
        summary.append("Row Sums:\n");
        rowSums.forEach((incurredDate, sum) -> summary.append(incurredDate).append(": ").append(roundToPrecision(sum)).append("\n"));

        summary.append("\nColumn Sums:\n");
        columnSums.forEach((exposureDate, sum) -> summary.append(exposureDate).append(": ").append(roundToPrecision(sum)).append("\n"));

        summary.append("\nTotal Sum: ").append(roundToPrecision(totalSum));

        return summary.toString();
    }

    public List<LocalDate> getExposureBuckets() {
        return entries.stream()
                      .map(ExposureMatrixEntry::getExposureDateBucket)
                      .distinct()
                      .sorted()
                      .toList();
    }

    public List<LocalDate> getIncurredBuckets() {
        return entries.stream()
                      .map(ExposureMatrixEntry::getIncurredDateBucket)
                      .distinct()
                      .sorted()
                      .toList();
    }
}
