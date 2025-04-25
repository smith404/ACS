package com.k2.acs.model;

import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

public class Calculator {

    public record ExposureMatrixEntry(LocalDate incurredDateBucket, LocalDate exposureDateBucket, double sum)
    {
    }

    public enum FactorType {
        WRITING,
        EARNING
    }

    @Getter
    private static boolean useCalendar = false;

    private static final Map<PatternElement.Type, Integer> typeToDaysMap = new EnumMap<>(PatternElement.Type.class);
    static {
        typeToDaysMap.put(PatternElement.Type.DAY, 1);
        typeToDaysMap.put(PatternElement.Type.WEEK, 7);
        typeToDaysMap.put(PatternElement.Type.MONTH, 30);
        typeToDaysMap.put(PatternElement.Type.QUARTER, 90);
        typeToDaysMap.put(PatternElement.Type.YEAR, 360);
    }
    public static void updateTypeToDays(PatternElement.Type type, int days) {
        typeToDaysMap.put(type, days);
    }

    public static void setUseCalendar(boolean useCalendar) {
        Calculator.useCalendar = useCalendar;
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

    private final int precision;
    private final Pattern pattern;

    public Calculator(int precision, Pattern pattern) {
        this.precision = precision;
        this.pattern = pattern;
    }

    public static int getDaysForType(PatternElement.Type type) {
        return typeToDaysMap.getOrDefault(type, 0);
    }

    public static int getDaysForTypeWithCalendar(PatternElement.Type type, LocalDate startDate) {
        if ((type == PatternElement.Type.MONTH || type == PatternElement.Type.QUARTER || type == PatternElement.Type.YEAR) && useCalendar) {
            LocalDate endDate = switch (type) {
                case MONTH -> startDate.plusMonths(1);
                case QUARTER -> startDate.plusMonths(3);
                case YEAR -> startDate.plusYears(1);
                default -> startDate;
            };
            return (int) java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate);
        }
        return typeToDaysMap.getOrDefault(type, 0);
    }

    public List<Factor> calculateDailyFactors(LocalDate startDate, FactorType factorType) {
        List<Factor> allFactors = new ArrayList<>();
        for (PatternElement element : pattern.getElements()) {
            List<Factor> factors = switch (factorType) {
                case WRITING -> element.generateWritingFactors(startDate);
                case EARNING -> element.generateEarningFactors(startDate);
            };
            allFactors.addAll(factors);
            startDate = startDate.plusDays(Calculator.getDaysForTypeWithCalendar(element.getType(), startDate)); 
        }
        return allFactors;
    }

    public List<Factor> applyUltimateValueToPattern(List<Factor> factors, UltimateValue ultimateValue) {
        return factors.stream()
                      .map(factor -> new Factor(
                            factor.getIncurredDate(),
                            factor.getDistribution(),
                            factor.getExposureDate(),
                            factor.getDistribution() * ultimateValue.getAmount()
                      ))
                      .toList();
    }

    public double sumValuesBetweenDates(List<Factor> factors, LocalDate startDate, LocalDate endDate) {
        return factors.stream()
                      .filter(factor -> !factor.getExposureDate().isBefore(startDate) && !factor.getExposureDate().isAfter(endDate))
                      .mapToDouble(Factor::getValue)
                      .sum();
    }

    public List<CashFlow> generateCashFlows(List<Factor> factors, LocalDate startDate, List<LocalDate> endDates, boolean toEnd) {
        List<CashFlow> cashFlows = new ArrayList<>();
        for (LocalDate endDate : endDates)
        {
            if (endDate.isAfter(startDate.minusDays(1)))
            {
                double sum = roundToPrecision(sumValuesBetweenDates(factors, startDate, endDate));
                CashFlow cashFlow = new CashFlow(toEnd ? endDate : startDate, sum);
                cashFlows.add(cashFlow);
                startDate = endDate.plusDays(1);
            }
        }
        return cashFlows;
    }

    public List<Factor> combineDailyFactors(Pattern pattern1, Pattern pattern2, LocalDate startDate, FactorType factorType) {
        List<Factor> factors1 = new Calculator(precision, pattern1).calculateDailyFactors(startDate, factorType);
        List<Factor> factors2 = new Calculator(precision, pattern2).calculateDailyFactors(startDate, factorType);

        int size = Math.min(factors1.size(), factors2.size());
        List<Factor> combinedFactors = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            Factor factor1 = factors1.get(i);
            Factor factor2 = factors2.get(i);
            combinedFactors.add(new Factor(
                factor1.getIncurredDate(),
                factor1.getDistribution() * factor2.getDistribution(),
                factor1.getExposureDate(),
                factor1.getValue() * factor2.getValue()
            ));
        }
        return combinedFactors;
    }

    public List<Factor> normalizeFactors(List<Factor> factors) {
        double totalDistribution = factors.stream()
                                   .mapToDouble(Factor::getDistribution)
                                   .sum();
        if (totalDistribution == 0) {
            throw new IllegalArgumentException("Total value of factors cannot be zero for normalization.");
        }
        return factors.stream()
                      .map(factor -> new Factor(
                        factor.getIncurredDate(),
                          factor.getDistribution() / totalDistribution,
                          factor.getExposureDate(),
                          factor.getValue()
                      ))
                      .toList();
    }

    public double roundToPrecision(double value) {
        return BigDecimal.valueOf(value)
                         .setScale(precision, RoundingMode.HALF_UP)
                         .doubleValue();
    }

    public List<ExposureMatrixEntry> generateExposureMatrix(List<Factor> factors, LocalDate startDate, List<LocalDate> incurredDateBuckets, List<LocalDate> exposureDateBuckets, boolean toEnd) {
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

    public static String generateExposureMatrixTable(List<ExposureMatrixEntry> entries) {
        // Extract unique buckets for x-axis and y-axis
        List<LocalDate> exposureDateBuckets = entries.stream()
                                                     .map(ExposureMatrixEntry::exposureDateBucket)
                                                     .distinct()
                                                     .sorted()
                                                     .toList();
        List<LocalDate> incurredDateBuckets = entries.stream()
                                                     .map(ExposureMatrixEntry::incurredDateBucket)
                                                     .distinct()
                                                     .sorted()
                                                     .toList();

        // Build the table header
        StringBuilder table = new StringBuilder();
        table.append("Incurred \\ Exposure");
        for (LocalDate exposureDate : exposureDateBuckets) {
            table.append("\t").append(exposureDate);
        }
        table.append("\n");

        // Populate the table rows
        for (LocalDate incurredDate : incurredDateBuckets) {
            table.append(incurredDate);
            for (LocalDate exposureDate : exposureDateBuckets) {
                double sum = entries.stream()
                                    .filter(entry -> entry.incurredDateBucket().equals(incurredDate) &&
                                                     entry.exposureDateBucket().equals(exposureDate))
                                    .mapToDouble(ExposureMatrixEntry::sum)
                                    .sum();
                table.append("\t").append(sum);
            }
            table.append("\n");
        }

        return table.toString();
    }
}

