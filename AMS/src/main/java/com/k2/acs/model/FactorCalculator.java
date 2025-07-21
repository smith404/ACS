package com.k2.acs.model;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import java.util.logging.Logger;

@Getter
@Setter
public class FactorCalculator implements DateCriteriaSummable {
    private static final Logger logger = Logger.getLogger(FactorCalculator.class.getName());

    public enum FactorType {
        WRITING,
        EARNING
    }

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

    public static int getDaysForType(PatternElement.Type type) {
        return typeToDaysMap.getOrDefault(type, 0);
    }

    public static int getDaysForTypeWithCalendar(PatternElement.Type type, LocalDate startDate) {
        if ((type == PatternElement.Type.MONTH || type == PatternElement.Type.QUARTER || type == PatternElement.Type.YEAR)) {
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

    private boolean useCalendar = true;
    private boolean useLinear = false;
    private boolean fast = false;
    private LocalDate writtenDate = LocalDate.now();
    private final int precision;
    private final Pattern pattern;
    private List<Factor> allFactors;

    public FactorCalculator(int precision, Pattern pattern) {
        if (pattern == null) {
            throw new IllegalArgumentException("Pattern must not be null.");
        }
        if (precision < 0) {
            throw new IllegalArgumentException("Precision must be a non-negative integer.");
        }

        this.precision = precision;
        this.pattern = pattern;
    }

    public void generateDailyFactors(LocalDate startDate, FactorType factorType) {
        long startTime = System.currentTimeMillis();
        allFactors = new ArrayList<>();
        for (PatternElement element : pattern.getElements()) {
            List<Factor> factors = switch (factorType) {
                case WRITING -> element.generateWritingFactors(startDate);
                case EARNING -> element.generateEarningFactors(startDate, useCalendar, useLinear, fast);
            };
            // Only add factors with non-zero distribution
            factors.stream()
                    .filter(factor -> factor.getValue() != 0.0)
                    .forEach(factor -> {
                        if (factor.getExposureDate().isBefore(writtenDate)) {
                            factor.setWritten(true);
                        }
                        allFactors.add(factor);
                    });
            startDate = startDate.plusDays(FactorCalculator.getDaysForTypeWithCalendar(element.getType(), startDate));
        }
        long endTime = System.currentTimeMillis();
        if (logger.isLoggable(java.util.logging.Level.INFO)) {
            logger.info(String.format("Generation of daily factors took %d ms to generate %d factors.", (endTime - startTime), allFactors.size()));
        }
    }

    public List<Factor> applyUltimateValueToPattern(UltimateValue ultimateValue) {
        if (allFactors == null) {
            throw new IllegalStateException("allFactors must not be null before applying the ultimate value.");
        }
        return allFactors.stream()
                .map(factor -> new Factor(
                        factor.getIncurredDate(),
                        factor.getExposureDate(),
                        factor.getValue() * ultimateValue.getAmount(),
                        factor.isWritten()))
                .toList();
    }

    public void normalizeFactors() {
        if (allFactors == null) {
            throw new IllegalStateException("allFactors must not be null before normalization.");
        }
        double totalDistribution = allFactors.stream()
                .mapToDouble(Factor::getValue)
                .sum();
        if (totalDistribution == 0) {
            throw new IllegalArgumentException("Total value of factors cannot be zero for normalization.");
        }

        allFactors = allFactors.stream()
                .map(factor -> new Factor(
                        factor.getIncurredDate(),
                        factor.getExposureDate(),
                        factor.getValue() / totalDistribution
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