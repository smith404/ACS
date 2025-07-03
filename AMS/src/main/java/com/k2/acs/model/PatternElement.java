package com.k2.acs.model;

import lombok.Data;
import lombok.AllArgsConstructor;
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
    private double distribution = 0;
    private double initialDistribution = 0;
    private int riskAttachingDuration = 0;

    public PatternElement(double initialDistribution, double distribution, Type type, int riskAttachingDuration) {
        this.initialDistribution = initialDistribution;
        this.distribution = distribution;
        this.type = type;
        this.riskAttachingDuration = riskAttachingDuration;
    }

    public PatternElement(double distribution, Type type) {
        this.distribution = distribution;
        this.type = type;
    }

    public int getNormlizedRiskAttachingDuration(LocalDate initialDate, int riskAttachingDuration) {
        if (initialDate == null) {
            return 0;
        }
        int years = riskAttachingDuration / 360;
        int months = (riskAttachingDuration % 360) / 30;
        int days = riskAttachingDuration % 30;
        LocalDate normalizedDate = initialDate.plusYears(years).plusMonths(months).plusDays(days);
        return (int) java.time.temporal.ChronoUnit.DAYS.between(initialDate, normalizedDate);
    }


    public List<Factor> generateWritingFactors(LocalDate startDate) {
        LocalDate originDate = startDate;
        int elementDays = FactorCalculator.getDaysForTypeWithCalendar(this.type, startDate);
        List<Factor> factors = new ArrayList<>();
        double factorDistribution = this.distribution / elementDays; 

        for (int i = 0; i < elementDays; i++) {
            if (i == 0) {
                factors.add(new Factor(originDate, factorDistribution + this.initialDistribution, startDate.plusDays(i)));
            } else {
                factors.add(new Factor(originDate, factorDistribution, startDate.plusDays(i)));
            }
        }

        return factors;
    }

    public List<Factor> generateEarningFactors(LocalDate startDate, boolean useCalendar) {
        LocalDate originDate = startDate;
        int riskDuration = this.riskAttachingDuration;
        if (useCalendar) {
            riskDuration = getNormlizedRiskAttachingDuration(originDate, riskDuration);
        }
        int elementDays = FactorCalculator.getDaysForTypeWithCalendar(this.type, startDate);
        List<Factor> factors = new ArrayList<>();
        double factorDistribution = this.distribution / (riskDuration); 
        double initialFactorDistribution = this.initialDistribution / riskDuration; 

        for (int i = 0; i < elementDays + riskDuration; i++) {
            if (i < elementDays) {
                factors.add(new Factor(originDate, (factorDistribution/2) + initialFactorDistribution, startDate.plusDays(i)));
            } else if (i < riskDuration) {
                factors.add(new Factor(originDate, factorDistribution + initialFactorDistribution, startDate.plusDays(i)));
            } else {
                factors.add(new Factor(originDate, (factorDistribution/2) , startDate.plusDays(i)));
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
