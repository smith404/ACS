package com.k2.acs.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class PatternElement {
    private double distribution;
    private Pattern.Type type;
    private Pattern parentPattern; // Reference to the parent pattern

    public List<Factor> generateFactors(LocalDate startDate) {
        int days = Pattern.getDaysForType(this.type);
        List<Factor> factors = new ArrayList<>();
        double factorDistribution = this.distribution / days; // Calculate distribution per factor

        for (int i = 0; i < days; i++) {
            factors.add(new Factor(factorDistribution, startDate.plusDays(i)));
        }

        return factors;
    }
}
