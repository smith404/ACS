package com.k2.acs;

import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.Calculator.FactorType;
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
        UltimateValue ultimateValue = new UltimateValue(UltimateValue.Type.PREMIUM, 100);
        ultimateValue.addProperty("TOA", "A1100");

        // Print the UltimateValue object
        System.out.println(ultimateValue);

        // Create a Pattern object
        Pattern pattern = new Pattern();
        pattern.setType("INCR");
        pattern.setDuration(365);

        // Add two PatternElements of type MONTH with a distribution of 0.5 each
        PatternElement element1 = new PatternElement(0.1284, 0.2179, PatternElement.Type.QUARTER);
        PatternElement element2 = new PatternElement(0.2179, PatternElement.Type.QUARTER);
        PatternElement element3 = new PatternElement(0.2179, PatternElement.Type.QUARTER);
        PatternElement element4 = new PatternElement(0.2179, PatternElement.Type.QUARTER);
        pattern.addElement(element1);
        pattern.addElement(element2);
        pattern.addElement(element3);
        pattern.addElement(element4);

        // Use February 21, 2024, as the start date
        LocalDate startDate = LocalDate.of(2024, 7, 1);

        Calculator calculator = new Calculator(6, pattern);
        Calculator.setUseCalendar(true);

        List<Factor> factors = calculator.calculateDailyFactors(startDate, Calculator.FactorType.EARNING);

        factors = calculator.applyUltimateValueToPattern(factors, ultimateValue);
        //factors.forEach(factor -> System.out.println(factor.toString()));

        List<CashFlow> cashFlows = calculator.generateCashFlows(factors, LocalDate.of(2024, 1, 1), Calculator.getStartDatesBetween(2024,2026, PatternElement.Type.QUARTER));

        cashFlows.forEach(cashFlow -> System.out.println(cashFlow.toString()));

    }
}
