package com.k2.acs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.Pattern;
import com.k2.acs.model.PatternElement;
import com.k2.acs.model.Factor;
import com.k2.acs.model.BestEstimateCashFlow;
import com.k2.acs.model.Calculator;
import com.k2.acs.model.CashFlow;
import com.k2.acs.model.ClosingSteeringParameter;

import java.io.File;
import java.io.FileInputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static Logger getLogger() {
        return logger;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            getLogger().warning("Please provide the path to the JSON configuration file as the first argument.");
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
            LocalDate startDate = config.getInsuredPeriodStartDateAsLocalDate();

            Calculator.setUseCalendar(config.isCalendar());
            Calculator calculator = new Calculator(config.getPrecision(), pattern);

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
                cashFlow.setCurrency(config.getCurrency());
                cashFlow.addProperty("PATTERN_TYPE", config.getFactor());
                if (cashFlow.getAmount() != 0) {
                    if (cashFlow.getIncurredDate().isBefore(lbd)) {
                        sumBeforeLbd += cashFlow.getAmount();
                    } else {
                        sumAfterLbd += cashFlow.getAmount();
                    }
                }
            }

            BestEstimateCashFlow bestEstimateCashFlow = new BestEstimateCashFlow();
            bestEstimateCashFlow.addProperty("Valuation", "BASELINE");
            bestEstimateCashFlow.addProperty("CRE", config.getToa());
            bestEstimateCashFlow.addProperty("Factor", config.getFactor());
            bestEstimateCashFlow.loadCashFlows(cashFlows);
            bestEstimateCashFlow.sortCashFlows();

            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info(bestEstimateCashFlow.toString());
            }

            // Print the rounded sums
            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("Sum of cash flows before LBD: " + calculator.roundToPrecision(sumBeforeLbd));
                getLogger().info("Sum of cash flows after LBD: " + calculator.roundToPrecision(sumAfterLbd));
            }

            String csvFilePath = "csp.csv";
            try (FileInputStream csvInputStream = new FileInputStream(csvFilePath)) {
                List<ClosingSteeringParameter> units = new ArrayList<>(ClosingSteeringParameter.parseUnitsFromStream(csvInputStream, true, ","));
                ClosingSteeringParameter.validate(units);

                for (ClosingSteeringParameter unit : units) {
                    if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                        getLogger().info(unit.toString());
                    }
                }

                List<PatternElement> patternElements = ClosingSteeringParameter.toPatternElements(units);
                for (PatternElement patternElement : patternElements) {
                    if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                        getLogger().info(patternElement.toString());
                    }
                }

                units = ClosingSteeringParameter.convertQuartersToMonths(units);
                for (ClosingSteeringParameter unit : units) {
                    if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                        getLogger().info(unit.toString());
                    }
                }

                units = ClosingSteeringParameter.convertMonthsToQuarters(units);
                for (ClosingSteeringParameter unit : units) {
                    if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                        getLogger().info(unit.toString());
                    }
                }

            } catch (Exception e) {
                getLogger().warning("Error reading or parsing csp.csv: " + e.getMessage());
            }

        } catch (Exception e) {
            getLogger().warning("Error processing the configuration file: " + e.getMessage());
        }
    }
}
