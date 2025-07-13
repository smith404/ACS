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
        int upFrontDuration = this.initialDuration;
        int shareDuration = this.distributionDuration;

        if (useCalendar) {
            upFrontDuration = getNormalizedDuration(startDate, upFrontDuration);
            shareDuration = getNormalizedDuration(startDate, shareDuration);
        }

        int elementDays = FactorCalculator.getDaysForTypeWithCalendar(this.type, startDate);

        if (elementDays <= 0) {
            elementDays = 1;
        }

        if (upFrontDuration < elementDays) {
            upFrontDuration = elementDays;
        }

        if (shareDuration < elementDays) {
            shareDuration = elementDays;
        }

        List<Factor> factors = new ArrayList<>();
        double initialFactorDistribution = this.initial / upFrontDuration;
        double factorDistribution = this.distribution / shareDuration;
        double scaleFactor = 1 / (double) elementDays;

        double lastFactorValue = 0;
        double factorValue;
        for (int i = 0; i < shareDuration; i++) {
            if (i < elementDays) {
                if (!useLinear) {
                    int runInDay = i + 1;
                    double runInFactor = (runInDay * scaleFactor);
                    factorValue = (factorDistribution / 2) * runInFactor;
                } else {
                    factorValue = factorDistribution / 2;
                }
                factors.add(new Factor(startDate, startDate.plusDays(i), initialFactorDistribution));
                factors.add(new Factor(startDate.plusDays(i), startDate.plusDays(i), factorValue + lastFactorValue));
                factors.add(new Factor(startDate.plusDays(i), startDate.plusDays((long) shareDuration + elementDays - i - 1), factorValue + lastFactorValue));
                if (!useLinear) {
                    lastFactorValue = factorValue;
                }
            } else {
                int offset = i % (elementDays-1);
                factors.add(new Factor(startDate, startDate.plusDays(i), initialFactorDistribution));
                factors.add(new Factor(startDate.plusDays(offset), startDate.plusDays(i), factorDistribution));
                lastFactorValue = 0;
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
}
