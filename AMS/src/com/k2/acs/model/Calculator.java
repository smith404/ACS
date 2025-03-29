package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Calculator {

    private int precision = 6;

    public List<Factor> applyUltimateValueToPattern(Pattern pattern, UltimateValue ultimateValue, LocalDate startDate) {
        List<Factor> factors = pattern.iterateElementsWithStartDate(startDate);
        return factors.stream()
                      .map(factor -> new Factor(factor.getDistribution(), factor.getDate(),
                          BigDecimal.valueOf(factor.getDistribution() * ultimateValue.getAmount())
                                    .setScale(precision, RoundingMode.HALF_UP)
                                    .doubleValue()
                          ))
                      .collect(Collectors.toList());
    }

}
