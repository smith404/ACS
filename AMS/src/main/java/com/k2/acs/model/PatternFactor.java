package com.k2.acs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

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
public class PatternFactor {

    @Getter
    public enum Type {
        DAY(1),
        WEEK(7),
        MONTH(30),
        QUARTER(90),
        YEAR(360);
        
        private final int defaultDays;
        
        Type(int defaultDays) {
            this.defaultDays = defaultDays;
        }

    }

    private static final int DAYS_PER_YEAR = 360;
    private static final int DAYS_PER_MONTH = 30;
    private static final int MONTHS_PER_QUARTER = 3;

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

    private final String uuid = UUID.randomUUID().toString();
    private Type type;
    private int elementDays;
    private int normalizedElementDays;
    private double upFront;
    private double distribution;
    private int upFrontDuration;
    private int normalizedUpFrontDuration;
    private int distributionDuration;
    private int normalizedDistributionDuration;

    public PatternFactor(Type type, double upFront, double distribution, int upFrontDuration, int distributionDuration) {
        this.type = type;
        this.elementDays = type.getDefaultDays();
        this.normalizedElementDays = elementDays;
        this.upFront = upFront;
        this.distribution = distribution;
        this.upFrontDuration = upFrontDuration;
        this.normalizedUpFrontDuration = upFrontDuration;
        this.distributionDuration = distributionDuration;
        this.normalizedDistributionDuration = distributionDuration;
    }

    public PatternFactor(Type type, double distribution, int distributionDuration) {
        this(type, 0, distribution, 0, distributionDuration);
    }

    public List<Factor> getFactors(LocalDate startDate, boolean useCalendar, boolean linear) {
        if (startDate == null) {
            return new ArrayList<>();
        }
        
        if (useCalendar) {
            normalizedElementDays = getNormalizedDuration(startDate, elementDays);
            normalizedUpFrontDuration = getNormalizedDuration(startDate, upFrontDuration);
            normalizedDistributionDuration = getNormalizedDuration(startDate, distributionDuration);
        }

        List<Factor> factors = new ArrayList<>(upFrontFactors(startDate));
        for(int i = 0; i < normalizedElementDays; i++) {
            factors.addAll(distributionFactors(startDate.plusDays(i)));
        }

        return factors;
    }

    private List<Factor> upFrontFactors(LocalDate incurredDate) {
        List<Factor> factors = new ArrayList<>();

        double dailyFactor = upFront / normalizedUpFrontDuration;
        for (int i = 0; i < normalizedUpFrontDuration; i++) {
            factors.add(new Factor(incurredDate, incurredDate.plusDays(i), dailyFactor, Factor.Type.UPFRONT));
        }

        return factors;
    }

    private List<Factor> distributionFactors(LocalDate incurredDate) {
        List<Factor> factors = new ArrayList<>();

        double dailyFactor = distribution / normalizedDistributionDuration / normalizedElementDays;
        for (int i = 0; i < normalizedDistributionDuration; i++) {
            if (i == 0) {
                factors.add(new Factor(incurredDate, incurredDate.plusDays(i), dailyFactor/2, Factor.Type.DIST));
                continue;
            }
            factors.add(new Factor(incurredDate, incurredDate.plusDays(i), dailyFactor, Factor.Type.DIST));
        }
        factors.add(new Factor(incurredDate, incurredDate.plusDays(normalizedDistributionDuration), dailyFactor/2, Factor.Type.DIST));

        return factors;
    }   
}