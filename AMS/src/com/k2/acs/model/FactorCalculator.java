package com.k2.acs.model;

import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

public class FactorCalculator implements DateCriteriaSummable {
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
        FactorCalculator.useCalendar = useCalendar;
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

    private final int precision;
    private final Pattern pattern;

    @Getter
    private List<Factor> allFactors;

    public FactorCalculator(int precision, Pattern pattern) {
        this.precision = precision;
        this.pattern = pattern;
    }

    private double roundToPrecision(double value) {
        return BigDecimal.valueOf(value)
                         .setScale(precision, RoundingMode.HALF_UP)
                         .doubleValue();
    }

    public List<Factor> calculateDailyFactors(LocalDate startDate, FactorType factorType) {
        allFactors = new ArrayList<>();
        for (PatternElement element : pattern.getElements()) {
            List<Factor> factors = switch (factorType) {
                case WRITING -> element.generateWritingFactors(startDate);
                case EARNING -> element.generateEarningFactors(startDate);
            };
            allFactors.addAll(factors);
            startDate = startDate.plusDays(FactorCalculator.getDaysForTypeWithCalendar(element.getType(), startDate)); 
        }
        return allFactors;
    }

    public void applyUltimateValueToPattern(UltimateValue ultimateValue) {
        if (allFactors == null) {
            throw new IllegalStateException("allFactors must not be null before applying the ultimate value.");
        }
        allFactors = allFactors.stream()
                               .map(factor -> new Factor(
                                     factor.getIncurredDate(),
                                     factor.getDistribution(),
                                     factor.getExposureDate(),
                                     factor.getDistribution() * ultimateValue.getAmount()
                               ))
                               .toList();
    }

    public double sumValuesBetweenDates(List<Factor> factors, LocalDate startDate, LocalDate endDate) {
        if (allFactors == null) {
            throw new IllegalStateException("allFactors must not be null before summing values.");
        }
        return factors.stream()
                      .filter(factor -> !factor.getExposureDate().isBefore(startDate) && !factor.getExposureDate().isAfter(endDate))
                      .mapToDouble(Factor::getValue)
                      .sum();
    }

    public List<CashFlow> generateCashFlows(LocalDate startDate, List<LocalDate> endDates, boolean toEnd) {
        if (allFactors == null) {
            throw new IllegalStateException("allFactors must not be null before generating cash flows.");
        }
        List<CashFlow> cashFlows = new ArrayList<>();
        for (LocalDate endDate : endDates)
        {
            if (endDate.isAfter(startDate.minusDays(1)))
            {
                double sum = roundToPrecision(sumValuesBetweenDates(allFactors, startDate, endDate));
                CashFlow cashFlow = new CashFlow(toEnd ? endDate : startDate, sum);
                cashFlows.add(cashFlow);
                startDate = endDate.plusDays(1);
            }
        }
        return cashFlows;
    }

    public List<Factor> combineDailyFactors(Pattern pattern1, Pattern pattern2, LocalDate startDate, FactorType factorType) {
        List<Factor> factors1 = new FactorCalculator(precision, pattern1).calculateDailyFactors(startDate, factorType);
        List<Factor> factors2 = new FactorCalculator(precision, pattern2).calculateDailyFactors(startDate, factorType);

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

    public void normalizeFactors() {
        if (allFactors == null) {
            throw new IllegalStateException("allFactors must not be null before normalization.");
        }
        double totalDistribution = allFactors.stream()
                                   .mapToDouble(Factor::getDistribution)
                                   .sum();
        if (totalDistribution == 0) {
            throw new IllegalArgumentException("Total value of factors cannot be zero for normalization.");
        }

        allFactors =  allFactors.stream()
                      .map(factor -> new Factor(
                        factor.getIncurredDate(),
                          factor.getDistribution() / totalDistribution,
                          factor.getExposureDate(),
                          factor.getValue()
                      ))
                      .toList();
    }

    public LocalDate getEarliestIncurredDate() {
        if (allFactors == null || allFactors.isEmpty()) {
            throw new IllegalStateException("allFactors must not be null or empty.");
        }
        return allFactors.stream()
                         .map(Factor::getIncurredDate)
                         .min(LocalDate::compareTo)
                         .orElseThrow(() -> new IllegalStateException("Unable to determine the earliest incurred date."));
    }

    public LocalDate getLatestIncurredDate() {
        if (allFactors == null || allFactors.isEmpty()) {
            throw new IllegalStateException("allFactors must not be null or empty.");
        }
        return allFactors.stream()
                         .map(Factor::getIncurredDate)
                         .max(LocalDate::compareTo)
                         .orElseThrow(() -> new IllegalStateException("Unable to determine the latest incurred date."));
    }

    public LocalDate getEarliestExposureDate() {
        if (allFactors == null || allFactors.isEmpty()) {
            throw new IllegalStateException("allFactors must not be null or empty.");
        }
        return allFactors.stream()
                         .map(Factor::getExposureDate)
                         .min(LocalDate::compareTo)
                         .orElseThrow(() -> new IllegalStateException("Unable to determine the earliest exposure date."));
    }

    public LocalDate getLatestExposureDate() {
        if (allFactors == null || allFactors.isEmpty()) {
            throw new IllegalStateException("allFactors must not be null or empty.");
        }
        return allFactors.stream()
                         .map(Factor::getExposureDate)
                         .max(LocalDate::compareTo)
                         .orElseThrow(() -> new IllegalStateException("Unable to determine the latest exposure date."));
    }

    @Override
    public double getSumByDateCriteria(LocalDate date, 
                                       String incurredDateComparison, 
                                       String exposureDateComparison) {
        return allFactors.stream()
                         .filter(factor -> compareDates(factor.getIncurredDate(), date, incurredDateComparison) &&
                                           compareDates(factor.getExposureDate(), date, exposureDateComparison))
                         .mapToDouble(Factor::getValue)
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