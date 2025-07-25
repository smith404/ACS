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
        for (PatternFactor element : pattern.getElements()) {
            List<Factor> elementFactors = element.getFactors(currentStartDate, true, false);
            processAndAddFactors(elementFactors);
            currentStartDate = currentStartDate.plusDays(element.getNormalizedElementDays());
            System.out.println("Processed element: " + element.getType() + " from " + currentStartDate);
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

    private void validateFactorGenerationInputs(LocalDate startDate, FactorType factorType) {
        if (startDate == null) {
            throw new IllegalArgumentException("Start date cannot be null");
        }
        if (factorType == null) {
            throw new IllegalArgumentException("Factor type cannot be null");
        }
    }

    private void processAndAddFactors(List<Factor> factors) {
        factors.stream()
                .filter(this::isSignificantFactor)
                .forEach(this::addFactor);
    }

    private boolean isSignificantFactor(Factor factor) {
        return Math.abs(factor.getValue()) > ZERO_THRESHOLD;
    }

    private void addFactor(Factor factor) {
        allFactors.add(factor);
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
                factor.getFactorType()
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