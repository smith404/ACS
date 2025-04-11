package com.k2.acs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.Pattern;
import com.k2.acs.model.PatternElement;
import com.k2.acs.model.Factor;
import com.k2.acs.model.Calculator;
import com.k2.acs.model.CashFlow;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the path to the JSON configuration file as the first argument.");
            return;
        }

        try {
            // Parse the JSON file into an AmsConfig object
            String configFilePath = args[0];
            ObjectMapper objectMapper = new ObjectMapper();
            AmsConfig config = objectMapper.readValue(new File(configFilePath), AmsConfig.class);

            // Create an UltimateValue object using AmsConfig
            UltimateValue ultimateValue = new UltimateValue(UltimateValue.Type.PREMIUM, config.getAmount());
            ultimateValue.addProperty("TOA", config.getToa());

            // Create a Pattern object using AmsConfig
            Pattern pattern = new Pattern();
            pattern.setType(config.getPatternType());
            pattern.setDuration(config.getDuration());

            // Add PatternElements from AmsConfig
            for (AmsConfig.Element element : config.getElements()) {
                PatternElement patternElement = new PatternElement(
                    element.getInitial(),
                    element.getDistribution(),
                    PatternElement.Type.valueOf(element.getType())
                );
                pattern.addElement(patternElement);
            }

            // Use the contract date from AmsConfig as the start date
            LocalDate startDate = config.getContractDateAsLocalDate();

            Calculator calculator = new Calculator(config.getPrecision(), pattern);
            Calculator.setUseCalendar(config.isCalendar());

            List<Factor> factors = calculator.calculateDailyFactors(startDate, Calculator.FactorType.valueOf(config.getFactor().toUpperCase()));

            factors = calculator.applyUltimateValueToPattern(factors, ultimateValue);

            List<LocalDate> endPoints =  Calculator.getEndDatesBetween(
                    config.getCashFlowStartAsLocalDate().getYear(),
                    config.getCashFlowEndAsLocalDate().getYear(),
                    PatternElement.Type.valueOf(config.getCashFlowFrequency().toUpperCase())
                );

            List<CashFlow> cashFlows = calculator.generateCashFlows(
                factors,
                config.getCashFlowStartAsLocalDate(),
                endPoints,
                config.isEndOfPeriod()
            );

            // Calculate the sum of cash flows before and after the lbd
            LocalDate lbd = config.getLbdAsLocalDate();
            double sumBeforeLbd = 0.0;
            double sumAfterLbd = 0.0;

            for (CashFlow cashFlow : cashFlows) {
                cashFlow.setValuation("BASELINE");
                cashFlow.setCRE(config.getToa());
                if (cashFlow.getAmount() != 0) {
                    if (cashFlow.getIncurredDate().isBefore(lbd)) {
                        sumBeforeLbd += cashFlow.getAmount();
                    } else {
                        sumAfterLbd += cashFlow.getAmount();
                    }
                    System.out.println(cashFlow.toString());
                }
            }

            // Print the rounded sums
            System.out.println("Sum of cash flows before LBD: " + calculator.roundToPrecision(sumBeforeLbd));
            System.out.println("Sum of cash flows after LBD: " + calculator.roundToPrecision(sumAfterLbd));

            
        } catch (Exception e) {
            System.err.println("Error processing the configuration file: " + e.getMessage());
        }
    }
}
