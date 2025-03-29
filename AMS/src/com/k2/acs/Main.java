package com.k2.acs;

import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.Pattern;
import com.k2.acs.model.PatternElement;
import com.k2.acs.model.Factor;
import com.k2.acs.model.Calculator;

import java.time.LocalDate;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // Create an UltimateValue object
        UltimateValue ultimateValue = new UltimateValue();
        ultimateValue.setAmount(1945.0);
        ultimateValue.setType(UltimateValue.Type.PREMIUM);
        ultimateValue.addProperty("TOA", "1100");
        ultimateValue.addProperty("UV_BASIS", 39);

        // Print the UltimateValue object
        System.out.println(ultimateValue);

        // Create a Pattern object
        Pattern pattern = new Pattern();
        pattern.setType("Monthly Pattern");

        // Add two PatternElements of type MONTH with a distribution of 0.5 each
        PatternElement element1 = new PatternElement(0.2, 0.4, Pattern.Type.MONTH);
        PatternElement element2 = new PatternElement(0.4, Pattern.Type.MONTH);
        pattern.addElement(element1);
        pattern.addElement(element2);

        // Use February 21, 2024, as the start date
        LocalDate startDate = LocalDate.of(2024, 2, 21);

        Calculator calculator = new Calculator();

        List<Factor> factors = calculator.applyUltimateValueToPattern(pattern, ultimateValue, startDate);

        // Print the results
        factors.forEach(factor -> System.out.println(factor.toString()));

    }
}
