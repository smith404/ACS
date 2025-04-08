package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Calculator {
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

    public static List<LocalDate> getQuarterStartDates(int year) {
        return IntStream.rangeClosed(1, 4) // Changed from range(0, 4) to rangeClosed(1, 4)
                        .mapToObj(quarter -> LocalDate.of(year, (quarter - 1) * 3 + 1, 1)) // Adjusted to include the first quarter
                        .collect(Collectors.toList());
    }

    public static List<LocalDate> getQuarterEndDates(int year) {
        return IntStream.rangeClosed(1, 4) // Changed from range(0, 4) to rangeClosed(1, 4)
                        .mapToObj(quarter -> LocalDate.of(year, quarter * 3, 1).withDayOfMonth(LocalDate.of(year, quarter * 3, 1).lengthOfMonth())) // Adjusted to include the first quarter
                        .collect(Collectors.toList());
    }

    private int precision = 6;
    private boolean useCalendar = false;

    public int getDaysForType(PatternElement.Type type) {
        return getDaysForType(type, LocalDate.now());
    }

    public int getDaysForType(PatternElement.Type type, LocalDate startDate) {
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

    public List<Factor> calculateDailyFactors(Pattern pattern, LocalDate startDate) {
        List<Factor> allFactors = new ArrayList<>();
        for (PatternElement element : pattern.getElements()) {
            List<Factor> factors = element.generateFactors(this, startDate);
            allFactors.addAll(factors);
            startDate = startDate.plusDays(factors.size()); // Increment startDate by the length of the factor list
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
