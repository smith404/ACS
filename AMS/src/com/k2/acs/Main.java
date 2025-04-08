package com.k2.acs;

import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.Pattern;
import com.k2.acs.model.PatternElement;
import com.k2.acs.model.Factor;
import com.k2.acs.model.Calculator;
import com.k2.acs.model.CashFlow;

import java.time.LocalDate;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // Create an UltimateValue object
        UltimateValue ultimateValue = new UltimateValue(UltimateValue.Type.PREMIUM, 1945.0);
        ultimateValue.addProperty("TOA", "A1100");

        // Print the UltimateValue object
        System.out.println(ultimateValue);

        // Create a Pattern object
        Pattern pattern = new Pattern();
        pattern.setType("INCR");

        // Add two PatternElements of type MONTH with a distribution of 0.5 each
        PatternElement element1 = new PatternElement(0.2, PatternElement.Type.QUARTER);
        PatternElement element2 = new PatternElement(0.3, PatternElement.Type.QUARTER);
        PatternElement element3 = new PatternElement(0.1, PatternElement.Type.QUARTER);
        PatternElement element4 = new PatternElement(0.4, PatternElement.Type.QUARTER);
        pattern.addElement(element1);
        pattern.addElement(element2);
        pattern.addElement(element3);
        pattern.addElement(element4);

        // Use February 21, 2024, as the start date
        LocalDate startDate = LocalDate.of(2024, 2, 21);

        Calculator calculator = new Calculator(6, true);

        List<Factor> factors = calculator.calculateDailyFactors(pattern, startDate);

        factors = calculator.applyUltimateValueToPattern(factors, ultimateValue);

        //factors.forEach(factor -> System.out.println(factor.toString()));

        List<CashFlow> cashFlows = calculator.generateCashFlows(factors, LocalDate.of(2024, 1, 1), Calculator.getQuarterEndDates(2024));

        cashFlows.forEach(cashFlow -> System.out.println(cashFlow.toString()));

        cashFlows = calculator.generateCashFlows(factors, LocalDate.of(2025, 1, 1), Calculator.getQuarterEndDates(2025));

        cashFlows.forEach(cashFlow -> System.out.println(cashFlow.toString()));
    }
}
