package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.math.BigDecimal;
import java.math.RoundingMode;

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
        return pattern.getElements().stream()
                      .flatMap(element -> element.generateFactors(this, startDate).stream())
                      .collect(Collectors.toList());
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

    public LocalDate addTypeToDate(LocalDate startDate, PatternElement.Type type) {
        int daysToAdd = getDaysForType(type);
        return startDate.plusDays(daysToAdd);
    }

    public LocalDate addQuarterToDate(LocalDate startDate) {
        return startDate.plusMonths(3);
    }

}
