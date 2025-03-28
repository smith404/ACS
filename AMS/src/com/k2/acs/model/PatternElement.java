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
}
