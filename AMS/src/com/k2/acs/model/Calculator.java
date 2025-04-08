package com.k2.acs.model;

import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

public class Calculator {
    public enum FactorType {
        WRITING,
        EARNING
    }

    private static final Map<PatternElement.Type, Integer> typeToDaysMap = new EnumMap<>(PatternElement.Type.class);
    private static boolean useCalendar = false;

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

    public static boolean isUseCalendar() {
        return useCalendar;
    }

    public static List<LocalDate> getQuarterEndDates(int year) {
        return IntStream.rangeClosed(1, 4)
                        .mapToObj(quarter -> LocalDate.of(year, quarter * 3, 1).withDayOfMonth(LocalDate.of(year, quarter * 3, 1).lengthOfMonth())) // Adjusted to include the first quarter
                        .collect(Collectors.toList());
    }

    public static List<LocalDate> getQuarterEndDates(int startYear, int endYear) {
        return IntStream.rangeClosed(startYear, endYear)
                        .boxed()
                        .flatMap(year -> getQuarterEndDates(year).stream())
                        .collect(Collectors.toList());
    }

    private int precision = 6;
    private Pattern pattern = null;

    public Calculator(int precision, Pattern pattern) {
        this.precision = precision;
        this.pattern = pattern;
    }

    public static int getDaysForType(PatternElement.Type type) {
        return getDaysForType(type, LocalDate.now());
    }

    public static int getDaysForType(PatternElement.Type type, LocalDate startDate) {
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
            startDate = startDate.plusDays(Calculator.getDaysForType(element.getType(), startDate)); 
        }
        return allFactors;
    }

    public List<Factor> applyUltimateValueToPattern(List<Factor> factors, UltimateValue ultimateValue) {
        return factors.stream()
                      .map(factor -> new Factor(
                          factor.getDistribution(),
                          factor.getDate(),
                          BigDecimal.valueOf(factor.getDistribution() * ultimateValue.getAmount())
                                    .setScale(precision, RoundingMode.HALF_UP)
                                    .doubleValue()
                      ))
                      .collect(Collectors.toList());
    }

    public double sumValuesBetweenDates(List<Factor> factors, LocalDate startDate, LocalDate endDate) {
        return factors.stream()
                      .filter(factor -> !factor.getDate().isBefore(startDate) && !factor.getDate().isAfter(endDate))
                      .mapToDouble(Factor::getValue)
                      .sum();
    }

    public List<CashFlow> generateCashFlows(List<Factor> factors, LocalDate startDate, List<LocalDate> dates) {
        List<CashFlow> cashFlows = new ArrayList<>();
        for (int i = 0; i < dates.size(); i++) {
            if (i!=0) startDate = dates.get(i-1);
            LocalDate endDate = dates.get(i);
            double sum = sumValuesBetweenDates(factors, startDate, endDate);
            cashFlows.add(new CashFlow(endDate, sum));
        }
        return cashFlows;
    }
}
