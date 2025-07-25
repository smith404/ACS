package com.k2.acs.model;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Calculates factors for insurance patterns, supporting both writing and earning factor types.
 * Factors represent the distribution of exposure and incurred amounts over time periods.
 */
@Getter
@Setter
public class FactorCalculator implements DateCriteriaSummable {
    
    private static final Logger logger = Logger.getLogger(FactorCalculator.class.getName());
    
    private static final double ZERO_THRESHOLD = 1e-10;
    private static final String FACTORS_NULL_ERROR = "Factors must be generated before this operation";
    private static final String FACTORS_EMPTY_ERROR = "Factors list cannot be empty for this operation";

    public enum FactorType {
        WRITING,
        EARNING
    }

    private static final Map<PatternElement.Type, Integer> TYPE_TO_DAYS_MAP = new EnumMap<>(PatternElement.Type.class);

    static {
        TYPE_TO_DAYS_MAP.put(PatternElement.Type.DAY, 1);
        TYPE_TO_DAYS_MAP.put(PatternElement.Type.WEEK, 7);
        TYPE_TO_DAYS_MAP.put(PatternElement.Type.MONTH, 30);
        TYPE_TO_DAYS_MAP.put(PatternElement.Type.QUARTER, 90);
        TYPE_TO_DAYS_MAP.put(PatternElement.Type.YEAR, 360);
    }

    // Configuration properties
    private boolean useCalendar = true;
    private boolean useLinear = false;
    private boolean fast = false;
    private LocalDate writtenDate = LocalDate.now();
    
    // Core components
    private final int precision;
    private final Pattern pattern;
    private List<Factor> allFactors;

    public FactorCalculator(int precision, Pattern pattern) {
        validateConstructorInputs(precision, pattern);
        this.precision = precision;
        this.pattern = pattern;
    }

    /**
     * Updates the standard days mapping for a given pattern element type.
     * @param type The pattern element type
     * @param days Number of days for this type
     */
    public static void updateTypeToDays(PatternElement.Type type, int days) {
        if (type == null) {
            throw new IllegalArgumentException("Pattern element type cannot be null");
        }
        if (days <= 0) {
            throw new IllegalArgumentException("Days must be positive");
        }
        TYPE_TO_DAYS_MAP.put(type, days);
    }

    /**
     * Gets the standard number of days for a pattern element type.
     * @param type The pattern element type
     * @return Number of days, or 0 if type is not recognized
     */
    public static int getDaysForType(PatternElement.Type type) {
        return TYPE_TO_DAYS_MAP.getOrDefault(type, 0);
    }

    /**
     * Gets the actual calendar days for a pattern element type starting from a specific date.
     * For time periods like MONTH, QUARTER, and YEAR, this accounts for varying calendar lengths.
     * @param type The pattern element type
     * @param startDate The starting date for calculation
     * @return Actual calendar days for the period
     */
    public static int getDaysForTypeWithCalendar(PatternElement.Type type, LocalDate startDate) {
        if (startDate == null) {
            throw new IllegalArgumentException("Start date cannot be null");
        }
        
        if (isCalendarSensitiveType(type)) {
            LocalDate endDate = calculateEndDateForType(type, startDate);
            return (int) java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate);
        }
        
        return TYPE_TO_DAYS_MAP.getOrDefault(type, 0);
    }

    /**
     * Generates daily factors for the entire pattern based on the specified factor type.
     * This is the main entry point for factor calculation.
     * @param startDate Starting date for factor generation
     * @param factorType Type of factors to generate (WRITING or EARNING)
     */
    public void generateDailyFactors(LocalDate startDate, FactorType factorType) {
        validateFactorGenerationInputs(startDate, factorType);
        
        long startTime = System.currentTimeMillis();
        allFactors = new ArrayList<>();
        
        LocalDate currentStartDate = startDate;
        for (PatternElement element : pattern.getElements()) {
            List<Factor> elementFactors = generateElementFactors(element, currentStartDate, factorType);
            processAndAddFactors(elementFactors);
            currentStartDate = advanceToNextElement(element, currentStartDate);
        }
        
        logFactorGenerationComplete(startTime);
    }

    /**
     * Applies an ultimate value multiplier to all generated factors.
     * @param ultimateValue The ultimate value to apply
     * @return New list of factors with ultimate value applied
     */
    public List<Factor> applyUltimateValueToPattern(UltimateValue ultimateValue) {
        validateFactorsExist("apply ultimate value");
        validateUltimateValue(ultimateValue);
        
        return allFactors.stream()
                .map(factor -> createFactorWithUltimateValue(factor, ultimateValue))
                .toList();
    }

    /**
     * Normalizes all factors so their total sum equals 1.0.
     * This is useful for creating percentage distributions.
     */
    public void normalizeFactors() {
        validateFactorsExist("normalize factors");
        
        double totalDistribution = calculateTotalDistribution();
        validateNonZeroTotal(totalDistribution);
        
        allFactors = allFactors.stream()
                .map(factor -> createNormalizedFactor(factor, totalDistribution))
                .toList();
    }

    /**
     * Gets the earliest incurred date from all generated factors.
     * @return The earliest incurred date
     */
    public LocalDate getEarliestIncurredDate() {
        validateFactorsExistAndNotEmpty("determine earliest incurred date");
        return allFactors.stream()
                .map(Factor::getIncurredDate)
                .min(LocalDate::compareTo)
                .orElseThrow(() -> new IllegalStateException("Unable to determine the earliest incurred date"));
    }

    /**
     * Gets the latest incurred date from all generated factors.
     * @return The latest incurred date
     */
    public LocalDate getLatestIncurredDate() {
        validateFactorsExistAndNotEmpty("determine latest incurred date");
        return allFactors.stream()
                .map(Factor::getIncurredDate)
                .max(LocalDate::compareTo)
                .orElseThrow(() -> new IllegalStateException("Unable to determine the latest incurred date"));
    }

    /**
     * Gets the earliest exposure date from all generated factors.
     * @return The earliest exposure date
     */
    public LocalDate getEarliestExposureDate() {
        validateFactorsExistAndNotEmpty("determine earliest exposure date");
        return allFactors.stream()
                .map(Factor::getExposureDate)
                .min(LocalDate::compareTo)
                .orElseThrow(() -> new IllegalStateException("Unable to determine the earliest exposure date"));
    }

    /**
     * Gets the latest exposure date from all generated factors.
     * @return The latest exposure date
     */
    public LocalDate getLatestExposureDate() {
        validateFactorsExistAndNotEmpty("determine latest exposure date");
        return allFactors.stream()
                .map(Factor::getExposureDate)
                .max(LocalDate::compareTo)
                .orElseThrow(() -> new IllegalStateException("Unable to determine the latest exposure date"));
    }

    @Override
    public double getSumByDateCriteria(LocalDate date, String incurredDateComparison, String exposureDateComparison) {
        validateDateCriteriaInputs(date, incurredDateComparison, exposureDateComparison);
        
        return allFactors.stream()
                .filter(factor -> matchesDateCriteria(factor, date, incurredDateComparison, exposureDateComparison))
                .mapToDouble(Factor::getValue)
                .sum();
    }

    // Private helper methods
    
    private static void validateConstructorInputs(int precision, Pattern pattern) {
        if (pattern == null) {
            throw new IllegalArgumentException("Pattern must not be null");
        }
        if (precision < 0) {
            throw new IllegalArgumentException("Precision must be a non-negative integer");
        }
    }

    private static boolean isCalendarSensitiveType(PatternElement.Type type) {
        return type == PatternElement.Type.MONTH || 
               type == PatternElement.Type.QUARTER || 
               type == PatternElement.Type.YEAR;
    }

    private static LocalDate calculateEndDateForType(PatternElement.Type type, LocalDate startDate) {
        return switch (type) {
            case MONTH -> startDate.plusMonths(1);
            case QUARTER -> startDate.plusMonths(3);
            case YEAR -> startDate.plusYears(1);
            default -> startDate;
        };
    }

    private void validateFactorGenerationInputs(LocalDate startDate, FactorType factorType) {
        if (startDate == null) {
            throw new IllegalArgumentException("Start date cannot be null");
        }
        if (factorType == null) {
            throw new IllegalArgumentException("Factor type cannot be null");
        }
    }

    private List<Factor> generateElementFactors(PatternElement element, LocalDate startDate, FactorType factorType) {
        return switch (factorType) {
            case WRITING -> element.generateWritingFactors(startDate);
            case EARNING -> element.generateEarningFactors(startDate, useCalendar, useLinear, fast);
        };
    }

    private void processAndAddFactors(List<Factor> factors) {
        factors.stream()
                .filter(this::isSignificantFactor)
                .forEach(this::addFactorWithWrittenStatus);
    }

    private boolean isSignificantFactor(Factor factor) {
        return Math.abs(factor.getValue()) > ZERO_THRESHOLD;
    }

    private void addFactorWithWrittenStatus(Factor factor) {
        if (factor.getExposureDate().isBefore(writtenDate)) {
            factor.setWritten(true);
        }
        allFactors.add(factor);
    }

    private LocalDate advanceToNextElement(PatternElement element, LocalDate currentDate) {
        int daysToAdvance = getDaysForTypeWithCalendar(element.getType(), currentDate);
        return currentDate.plusDays(daysToAdvance);
    }

    private void logFactorGenerationComplete(long startTime) {
        if (logger.isLoggable(java.util.logging.Level.INFO)) {
            long duration = System.currentTimeMillis() - startTime;
            logger.info(String.format("Generation of daily factors took %d ms to generate %d factors", 
                    duration, allFactors.size()));
        }
    }

    private void validateFactorsExist(String operation) {
        if (allFactors == null) {
            throw new IllegalStateException(FACTORS_NULL_ERROR + ": " + operation);
        }
    }

    private void validateFactorsExistAndNotEmpty(String operation) {
        validateFactorsExist(operation);
        if (allFactors.isEmpty()) {
            throw new IllegalStateException(FACTORS_EMPTY_ERROR + ": " + operation);
        }
    }

    private void validateUltimateValue(UltimateValue ultimateValue) {
        if (ultimateValue == null) {
            throw new IllegalArgumentException("Ultimate value cannot be null");
        }
    }

    private Factor createFactorWithUltimateValue(Factor factor, UltimateValue ultimateValue) {
        return new Factor(
                factor.getIncurredDate(),
                factor.getExposureDate(),
                factor.getValue() * ultimateValue.getAmount(),
                factor.getFactorType(),
                factor.isWritten()
        );
    }

    private double calculateTotalDistribution() {
        return allFactors.stream().mapToDouble(Factor::getValue).sum();
    }

    private void validateNonZeroTotal(double total) {
        if (Math.abs(total) < ZERO_THRESHOLD) {
            throw new IllegalArgumentException("Total value of factors cannot be zero for normalization");
        }
    }

    private Factor createNormalizedFactor(Factor factor, double totalDistribution) {
        return new Factor(
                factor.getIncurredDate(),
                factor.getExposureDate(),
                factor.getValue() / totalDistribution,
                factor.getFactorType()
        );
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

    private boolean matchesDateCriteria(Factor factor, LocalDate date, String incurredComparison, String exposureComparison) {
        return compareDates(factor.getIncurredDate(), date, incurredComparison) &&
               compareDates(factor.getExposureDate(), date, exposureComparison);
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