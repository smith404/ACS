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
        pattern.setDuration(360);

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
        LocalDate startDate = LocalDate.of(2024, 2, 21);

        Calculator calculator = new Calculator(6, pattern);
        Calculator.setUseCalendar(false);

        List<Factor> factors = calculator.calculateDailyFactors(startDate, Calculator.FactorType.EARNING);

        factors = calculator.applyUltimateValueToPattern(factors, ultimateValue);
        //factors.forEach(factor -> System.out.println(factor.toString()));

        List<CashFlow> cashFlows = calculator.generateCashFlows(factors, LocalDate.of(2024, 1, 1), Calculator.getMonthEndDates(2024,2026));

        cashFlows.forEach(cashFlow -> System.out.println(cashFlow.toString()));

        // Create a Pattern object
        Pattern riskPattern = new Pattern();
        pattern.setType("SEAS");
        pattern.setDuration(360);

        // Add two PatternElements of type MONTH with a distribution of 0.5 each
        PatternElement riskElement1 = new PatternElement(1, PatternElement.Type.MONTH);
        PatternElement riskElement2 = new PatternElement(1, PatternElement.Type.MONTH);
        PatternElement riskElement3 = new PatternElement(2, PatternElement.Type.MONTH);
        PatternElement riskElement4 = new PatternElement(3, PatternElement.Type.MONTH);
        PatternElement riskElement5 = new PatternElement(3, PatternElement.Type.MONTH);
        PatternElement riskElement6 = new PatternElement(1, PatternElement.Type.MONTH);
        PatternElement riskElement7 = new PatternElement(1, PatternElement.Type.MONTH);
        PatternElement riskElement8 = new PatternElement(1, PatternElement.Type.MONTH);
        PatternElement riskElement9 = new PatternElement(4, PatternElement.Type.MONTH);
        PatternElement riskElement10 = new PatternElement(3, PatternElement.Type.MONTH);
        PatternElement riskElement11 = new PatternElement(2, PatternElement.Type.MONTH);
        PatternElement riskElement12 = new PatternElement(1, PatternElement.Type.MONTH);
        riskPattern.addElement(riskElement1);
        riskPattern.addElement(riskElement2);
        riskPattern.addElement(riskElement3);
        riskPattern.addElement(riskElement4);
        riskPattern.addElement(riskElement5);
        riskPattern.addElement(riskElement6);
        riskPattern.addElement(riskElement7);
        riskPattern.addElement(riskElement8);
        riskPattern.addElement(riskElement9);
        riskPattern.addElement(riskElement10);
        riskPattern.addElement(riskElement11);
        riskPattern.addElement(riskElement12);

        List<Factor> newFactors = calculator.combineDailyFactors(pattern, riskPattern, startDate, FactorType.EARNING);
        calculator.normalizeFactors(newFactors).forEach(factor -> System.out.println(factor.toString()));

    }
}
