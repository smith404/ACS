package com.k2.acs.model;

import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

public class FactorCalculator {

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
        List<Factor> allFactors = new ArrayList<>();
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

}