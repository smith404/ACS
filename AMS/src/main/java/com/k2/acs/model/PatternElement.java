package com.k2.acs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents a pattern element that defines how factors are distributed over time.
 * Each element has a type (DAY, WEEK, MONTH, QUARTER, YEAR), an initial value,
 * and a distribution value that gets spread over a duration.
 */
@Data
@AllArgsConstructor
public class PatternElement {
    
    public enum Type {
        DAY, WEEK, MONTH, QUARTER, YEAR
    }

    private static final int DAYS_PER_YEAR = 360;
    private static final int DAYS_PER_MONTH = 30;
    private static final int MONTHS_PER_QUARTER = 3;

    private final String uuid = UUID.randomUUID().toString();
    private Pattern parentPattern;
    private Type type;
    private double distribution;
    private double initial = 0;
    private int distributionDuration = 0;
    private int initialDuration = 0;

    public PatternElement(double initial, double distribution, Type type, int initialDuration, int distributionDuration) {
        this.initial = initial;
        this.distribution = distribution;
        this.type = type;
        this.distributionDuration = distributionDuration;
        this.initialDuration = initialDuration;
    }

    public PatternElement(double distribution, Type type) {
        this.distribution = distribution;
        this.type = type;
    }

    /**
     * Converts a duration in days (using 360-day year convention) to actual calendar days.
     * @param initialDate Starting date for the calculation
     * @param duration Duration in 360-day convention
     * @return Actual calendar days between initialDate and the calculated end date
     */
    public static int getNormalizedDuration(LocalDate initialDate, int duration) {
        if (initialDate == null) {
            return 0;
        }
        
        int years = duration / DAYS_PER_YEAR;
        int months = (duration % DAYS_PER_YEAR) / DAYS_PER_MONTH;
        int days = duration % DAYS_PER_MONTH;
        
        LocalDate normalizedDate = initialDate.plusYears(years).plusMonths(months).plusDays(days);
        return (int) java.time.temporal.ChronoUnit.DAYS.between(initialDate, normalizedDate);
    }

    /**
     * Generates writing factors where both incurred and exposure dates are the same.
     * The initial value is applied on the first day, and distribution is spread evenly.
     */
    public List<Factor> generateWritingFactors(LocalDate startDate) {
        int elementDays = getElementDays(startDate);
        List<Factor> factors = new ArrayList<>();
        
        if (elementDays == 1) {
            // Single day: combine initial and distribution
            factors.add(new Factor(startDate, startDate, this.initial + this.distribution, Factor.Type.DIST));
        } else {
            // Multiple days: initial on first day, then distribute remainder
            double dailyDistribution = this.distribution / elementDays;
            
            // First day gets initial plus its share of distribution
            factors.add(new Factor(startDate, startDate, this.initial + dailyDistribution, Factor.Type.DIST));
            
            // Remaining days get their share of distribution
            for (int i = 1; i < elementDays; i++) {
                LocalDate currentDate = startDate.plusDays(i);
                factors.add(new Factor(currentDate, currentDate, dailyDistribution, Factor.Type.DIST));
            }
        }

        return factors;
    }

    /**
     * Generates earning factors with complex distribution patterns including
     * initial upfront amounts and progressive distribution over time.
     */
    public List<Factor> generateEarningFactors(LocalDate startDate, boolean useCalendar, boolean useLinear, boolean fast) {
        int elementDays = getElementDays(startDate);
        DurationConfig durationConfig = calculateDurations(startDate, useCalendar, elementDays);
        
        List<Factor> factors = new ArrayList<>();
        
        generateInitialFactors(factors, startDate, durationConfig);
        generateDistributionFactors(factors, startDate, durationConfig, elementDays, useLinear, fast);
        
        return factors;
    }

    private int getElementDays(LocalDate startDate) {
        int days = FactorCalculator.getDaysForTypeWithCalendar(this.type, startDate);
        return Math.max(days, 1);
    }

    private DurationConfig calculateDurations(LocalDate startDate, boolean useCalendar, int elementDays) {
        int upFrontDuration = this.initialDuration;
        int shareDuration = this.distributionDuration;
        
        if (useCalendar) {
            upFrontDuration = getNormalizedDuration(startDate, upFrontDuration);
            shareDuration = getNormalizedDuration(startDate, shareDuration);
        }
        
        upFrontDuration = Math.max(upFrontDuration, 1);
        shareDuration = Math.max(shareDuration, 1);
        int totalDuration = Math.max(elementDays, Math.max(upFrontDuration, shareDuration));
        
        return new DurationConfig(upFrontDuration, shareDuration, totalDuration, elementDays);
    }

    private void generateInitialFactors(List<Factor> factors, LocalDate startDate, DurationConfig config) {
        if (this.initial <= 0) return;
        
        double dailyInitialDistribution = this.initial / config.upFrontDuration;
        
        for (int i = 0; i < config.upFrontDuration; i++) {
            factors.add(new Factor(startDate, startDate.plusDays(i), dailyInitialDistribution, Factor.Type.UPFRONT));
        }
    }

    private void generateDistributionFactors(List<Factor> factors, LocalDate startDate, DurationConfig config, 
                                           int elementDays, boolean useLinear, boolean fast) {
        double factorDistribution = this.distribution / config.shareDuration;
        double scaleFactor = 1.0 / elementDays;

        generateElementPeriodFactors(factors, startDate, config, elementDays, factorDistribution, scaleFactor, useLinear);
        generateExtendedPeriodFactors(factors, startDate, config, elementDays, factorDistribution, fast);
    }

    private void generateElementPeriodFactors(List<Factor> factors, LocalDate startDate, DurationConfig config,
                                            int elementDays, double factorDistribution, double scaleFactor, boolean useLinear) {
        double factorValue = factorDistribution / 2;
        double distributedFactorValue = (factorValue * elementDays) / sumDownToOne(elementDays);
        System.out.println("Factor Distribution: " + factorDistribution);
        System.out.println("Sum Down To One: " + sumDownToOne(elementDays));
        System.out.println("Factor Value: " + factorValue);
        System.out.println("Distributed Factor Value: " + distributedFactorValue);
        System.out.println("Element Days: " + elementDays);
        System.out.println("Share Duration: " + config.shareDuration);

        
        for (int i = 0; i < elementDays && i < config.shareDuration; i++) {
            if (useLinear) {
                distributedFactorValue = factorValue / (i + 1);
            }

            for (int j = 0; j <= i; j++) {
                // Forward factor (incurred to exposure)
                factors.add(new Factor(
                    startDate.plusDays(j), 
                    startDate.plusDays(i), 
                    distributedFactorValue,
                    Factor.Type.UPFRONT
                ));
                // Backward factor (exposure to incurred + share duration)
                factors.add(new Factor(
                    startDate.plusDays(elementDays - j - 1L), 
                    startDate.plusDays((long) config.shareDuration + i), 
                    distributedFactorValue,
                    Factor.Type.DIST
                ));
            }
        }
    }

    private double calculateFactorValue(int dayIndex, double factorDistribution, 
                                      double scaleFactor, boolean useLinear) {
        if (useLinear) {
            return factorDistribution / 2;
        } else {
            int runInDay = dayIndex + 1;
            double runInFactor = runInDay * scaleFactor;
            return (factorDistribution / 2) * runInFactor;
        }
    }

    private void generateExtendedPeriodFactors(List<Factor> factors, LocalDate startDate, DurationConfig config,
                                             int elementDays, double factorDistribution, boolean fast) {
        for (int i = elementDays; i < config.totalDuration && i < config.shareDuration; i++) {
            if (fast) {
                generateFastFactors(factors, startDate, elementDays, i, factorDistribution);
            } else {
                generateDetailedFactors(factors, startDate, elementDays, i, factorDistribution);
            }
        }
    }

    private void generateFastFactors(List<Factor> factors, LocalDate startDate, int elementDays, 
                                   int currentDay, double factorDistribution) {
        double quarterAlignment = getQuarterAlignmentPercentage(startDate, startDate.plusDays(elementDays - 1L));
        double alignmentFactor = quarterAlignment / 100.0;
        
        factors.add(new Factor(
            startDate, 
            startDate.plusDays(currentDay), 
            factorDistribution * alignmentFactor,
            Factor.Type.DIST
        ));
        
        factors.add(new Factor(
            startDate.plusDays(elementDays - 1L), 
            startDate.plusDays(currentDay), 
            factorDistribution * (1 - alignmentFactor),
            Factor.Type.DIST
        ));
    }

    private void generateDetailedFactors(List<Factor> factors, LocalDate startDate, int elementDays, 
                                       int currentDay, double factorDistribution) {
        double dailyFactorValue = factorDistribution / elementDays;
        
        for (int j = 0; j < elementDays; j++) {
            factors.add(new Factor(
                startDate.plusDays(j), 
                startDate.plusDays(currentDay), 
                dailyFactorValue,
                Factor.Type.DIST
            ));
        }
    }

    /**
     * Gets the length of this pattern element in days using standard day counts.
     */
    public int getLength() {
        return FactorCalculator.getDaysForType(this.type);
    }

    /**
     * Calculates the percentage alignment with calendar quarters for the given date range.
     * Returns 100% if the dates exactly match a quarter, or the percentage of days
     * until the next quarter start if a quarter boundary falls within the range.
     */
    public double getQuarterAlignmentPercentage(LocalDate startDate, LocalDate endDate) {
        LocalDate quarterStart = getQuarterStart(startDate);
        LocalDate quarterEnd = getQuarterEnd(quarterStart);
        
        // Perfect quarter alignment
        if (startDate.equals(quarterStart) && endDate.equals(quarterEnd)) {
            return 100.0;
        }
        
        // Check for quarter start within the date range
        LocalDate currentQuarterStart = quarterStart;
        while (!currentQuarterStart.isAfter(endDate)) {
            if (currentQuarterStart.isAfter(startDate) && !currentQuarterStart.isAfter(endDate)) {
                long totalDays = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate) + 1;
                long daysToQuarterStart = java.time.temporal.ChronoUnit.DAYS.between(startDate, currentQuarterStart);
                return (double) daysToQuarterStart / totalDays * 100.0;
            }
            currentQuarterStart = currentQuarterStart.plusMonths(MONTHS_PER_QUARTER);
        }
        
        return 0.0;
    }
    
    private LocalDate getQuarterStart(LocalDate date) {
        int quarter = (date.getMonthValue() - 1) / MONTHS_PER_QUARTER;
        int quarterStartMonth = quarter * MONTHS_PER_QUARTER + 1;
        return LocalDate.of(date.getYear(), quarterStartMonth, 1);
    }
    
    private LocalDate getQuarterEnd(LocalDate quarterStart) {
        return quarterStart.plusMonths(MONTHS_PER_QUARTER).minusDays(1);
    }

    /**
     * Returns the sum n + (n-1) + ... + 1 for a given n >= 1.
     */
    public static int sumDownToOne(int n) {
        if (n <= 0) return 1;
        return n * (n + 1) / 2;
    }

    /**
     * Internal class to hold duration configuration for factor generation.
     */
    @ToString
    private static class DurationConfig {
        final int upFrontDuration;
        final int shareDuration;
        final int totalDuration;
        final int elementDays;

        DurationConfig(int upFrontDuration, int shareDuration, int totalDuration, int elementDays) {
            this.upFrontDuration = upFrontDuration;
            this.shareDuration = shareDuration;
            this.totalDuration = totalDuration;
            this.elementDays = elementDays;
        }
    }
}
