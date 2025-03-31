package com.k2.acs.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
public class PatternElement {
    public static PatternElement fromFactors(List<Factor> factors, Pattern.Type type) {
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
    private Pattern.Type type;
    private Pattern parentPattern; // Reference to the parent pattern

    public PatternElement(double initialDistribution, double distribution, Pattern.Type type) {
        this.initialDistribution = initialDistribution;
        this.distribution = distribution;
        this.type = type;
    }

    public PatternElement(double distribution, Pattern.Type type) {
        this.distribution = distribution;
        this.type = type;
    }

    public List<Factor> generateFactors(LocalDate startDate) {
        int days = Pattern.getDaysForType(this.type);
        List<Factor> factors = new ArrayList<>();
        double factorDistribution = this.distribution / days; // Calculate distribution per factor

        for (int i = 0; i < days; i++) {
            if (i == 0) {
                factors.add(new Factor(factorDistribution + this.initialDistribution, startDate.plusDays(i)));
            } else {
                factors.add(new Factor(factorDistribution, startDate.plusDays(i)));
            }
        }

        return factors;
    }

    public int getLength() {
        return Pattern.getDaysForType(this.type);
    }

    public void weightPattern(double[] weights) {
        if (weights.length != this.getLength()) {
            throw new IllegalArgumentException("The number of weights must match the number of elements.");
        }

        double totalWeight = 0;
        for (double weight : weights) {
            totalWeight += weight;
        }

        double factorDistribution = this.distribution / totalWeight;

        for (int i = 0; i < weights.length; i++) {
            if (i == 0) {
                this.initialDistribution = factorDistribution * weights[i];
                this.distribution = factorDistribution * weights[i];
            } else {
                // Adjust the distribution for subsequent elements
                this.distribution = factorDistribution * weights[i];
            }
        }
    }
}
