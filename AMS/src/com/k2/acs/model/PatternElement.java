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
    public static PatternElement fromFactors(List<Factor> factors, Type type) {
        double totalDistribution = factors.stream()
                                          .mapToDouble(Factor::getDistribution)
                                          .sum();

        // If we have only one factor, there is no initial distribution
        double initialDistribution = 0;

        // If we have multiple factors, the initial distribution is the first factor's distribution
        if (factors.size() > 1) {
            initialDistribution = factors.get(0).getDistribution() - factors.get(1).getDistribution();
            totalDistribution = totalDistribution - initialDistribution;
        }
        return new PatternElement(initialDistribution, totalDistribution, type);
    }

    private final String uuid = UUID.randomUUID().toString(); // Generate UUID on creation
    private double distribution = 0;
    private double initialDistribution = 0;
    private Type type;
    private Pattern parentPattern;

    public PatternElement(double initialDistribution, double distribution, Type type) {
        this.initialDistribution = initialDistribution;
        this.distribution = distribution;
        this.type = type;
    }

    public PatternElement(double distribution, Type type) {
        this.distribution = distribution;
        this.type = type;
    }

    public List<Factor> generateWritingFactors(LocalDate startDate) {
        int elementDays = Calculator.getDaysForType(this.type, startDate);
        List<Factor> factors = new ArrayList<>();
        double factorDistribution = this.distribution / elementDays; 

        for (int i = 0; i < elementDays; i++) {
            if (i == 0) {
                factors.add(new Factor(factorDistribution + this.initialDistribution, startDate.plusDays(i)));
            } else {
                factors.add(new Factor(factorDistribution, startDate.plusDays(i)));
            }
        }

        return factors;
    }

    public List<Factor> generateEarningFactors(LocalDate startDate) {
        int elementDays = Calculator.getDaysForType(this.type, startDate);
        int contractDurationDays = parentPattern.getDuration();
        List<Factor> factors = new ArrayList<>();
        double factorDistribution = this.distribution / (contractDurationDays); 
        double initialFactorDistribution = this.initialDistribution / contractDurationDays; 

        for (int i = 0; i < elementDays + contractDurationDays; i++) {
            if (i < elementDays) {
                factors.add(new Factor((factorDistribution/2) + initialFactorDistribution, startDate.plusDays(i)));
            } else if (i < contractDurationDays) {
                factors.add(new Factor(factorDistribution + initialFactorDistribution, startDate.plusDays(i)));
            } else {
                factors.add(new Factor((factorDistribution/2) , startDate.plusDays(i)));
            }
        }

        return factors;
    }

    public int getLength() {
        return Calculator.getDaysForType(this.type);
    }

    public enum Type {
        DAY, WEEK, MONTH, QUARTER, YEAR
    }
}
