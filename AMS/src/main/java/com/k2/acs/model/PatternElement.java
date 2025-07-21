package com.k2.acs.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Data
@AllArgsConstructor
public class PatternElement {
    private final String uuid = UUID.randomUUID().toString();
    private Pattern parentPattern;
    private Type type;
    private double distribution;
    private double initial = 0;
    private int distributionDuration = 0;
    private int initialDuration = 0;

    public static int getNormalizedDuration(LocalDate initialDate, int duration) {
        if (initialDate == null) {
            return 0;
        }
        int years = duration / 360;
        int months = (duration % 360) / 30;
        int days = duration % 30;
        LocalDate normalizedDate = initialDate.plusYears(years).plusMonths(months).plusDays(days);
        return (int) java.time.temporal.ChronoUnit.DAYS.between(initialDate, normalizedDate);
    }

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

    public List<Factor> generateWritingFactors(LocalDate startDate) {
        int elementDays = FactorCalculator.getDaysForTypeWithCalendar(this.type, startDate);
        if (elementDays < 1) {
            elementDays = 1;
        } 
        List<Factor> factors = new ArrayList<>();
        double factorDistribution = this.distribution / elementDays;

        for (int i = 0; i < elementDays; i++) {
            if (i == 0) {
                factors.add(new Factor(startDate, startDate, this.initial));
                factors.add(new Factor(startDate, startDate, factorDistribution + this.initial));
            } else {
                factors.add(new Factor(startDate.plusDays(i), startDate.plusDays(i), factorDistribution));
            }
        }

        return factors;
    }

    public List<Factor> generateEarningFactors(LocalDate startDate, boolean useCalendar, boolean useLinear) {
        int elementDays = FactorCalculator.getDaysForTypeWithCalendar(this.type, startDate);
        if (elementDays < 1) {
            elementDays = 1;
        }

        System.out.println("Quick factor: " + getQuarterAlignmentPercentage(startDate, startDate.plusDays((long)elementDays - 1)));

        int upFrontDuration = this.initialDuration;
        int shareDuration = this.distributionDuration;
        if (useCalendar) {
            upFrontDuration = getNormalizedDuration(startDate, upFrontDuration);
            shareDuration = getNormalizedDuration(startDate, shareDuration);
        }
        if (upFrontDuration < 1) {
            upFrontDuration = 1;
        }
        if (shareDuration < 1) {
            shareDuration = 1;
        }
        int duration = Math.max(elementDays, Math.max(upFrontDuration, shareDuration));

        List<Factor> factors = new ArrayList<>();
        double initialFactorDistribution = this.initial / upFrontDuration;
        double factorDistribution = this.distribution / shareDuration;
        double scaleFactor = 1 / (double) elementDays;

        double lastFactorValue = 0;
        double factorValue;
        for (int i = 0; i < elementDays; i++) {
            if (!useLinear) {
                int runInDay = i + 1;
                double runInFactor = (runInDay * scaleFactor);
                factorValue = (factorDistribution / 2) * runInFactor;
            } else {
                factorValue = factorDistribution / 2;
            }
            if (i < upFrontDuration) {
                factors.add(new Factor(startDate, startDate.plusDays(i), initialFactorDistribution));
            } 
            if (i < shareDuration) {
                factors.add(new Factor(startDate.plusDays(i), startDate.plusDays(i), factorValue + lastFactorValue));
                factors.add(new Factor(startDate.plusDays((long) elementDays - i), startDate.plusDays(((long) shareDuration + (long) elementDays - i - 1L)), factorValue + lastFactorValue));
            }
            if (!useLinear) {
                lastFactorValue = factorValue;
            }
        }
        for (int i = elementDays; i < duration; i++) {
            if (i < upFrontDuration) {
                factors.add(new Factor(startDate, startDate.plusDays(i), initialFactorDistribution));
            } 
            if (i < shareDuration) {
                factorValue = factorDistribution / elementDays;
                for (int j = 0; j < elementDays; j++) {
                    factors.add(new Factor(startDate.plusDays(j), startDate.plusDays(i), factorValue));
                }
            }
        }

        return factors;
    }

    public int getLength() {
        return FactorCalculator.getDaysForType(this.type);
    }

    public enum Type {
        DAY, WEEK, MONTH, QUARTER, YEAR
    }

    public double getQuarterAlignmentPercentage(LocalDate startDate, LocalDate endDate) {
        // Check if start and end dates exactly match a calendar quarter
        LocalDate quarterStart = getQuarterStart(startDate);
        LocalDate quarterEnd = getQuarterEnd(quarterStart);
        
        if (startDate.equals(quarterStart) && endDate.equals(quarterEnd)) {
            return 100.0;
        }
        
        // Find if any quarter starts within the given date range
        LocalDate currentQuarterStart = quarterStart;
        while (!currentQuarterStart.isAfter(endDate)) {
            if (currentQuarterStart.isAfter(startDate) && !currentQuarterStart.isAfter(endDate)) {
                // Quarter starts within the range - calculate percentage
                long totalDays = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate) + 1;
                long daysToQuarterStart = java.time.temporal.ChronoUnit.DAYS.between(startDate, currentQuarterStart);
                return (double) daysToQuarterStart / totalDays * 100.0;
            }
            currentQuarterStart = currentQuarterStart.plusMonths(3);
        }
        
        return 0.0;
    }
    
    private LocalDate getQuarterStart(LocalDate date) {
        int quarter = (date.getMonthValue() - 1) / 3;
        int quarterStartMonth = quarter * 3 + 1;
        return LocalDate.of(date.getYear(), quarterStartMonth, 1);
    }
    
    private LocalDate getQuarterEnd(LocalDate quarterStart) {
        return quarterStart.plusMonths(3).minusDays(1);
    }
}
