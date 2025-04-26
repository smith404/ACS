package com.k2.acs;

import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.Pattern;
import com.k2.acs.model.PatternElement;
import com.k2.acs.model.Factor;
import com.k2.acs.model.BestEstimateCashFlow;
import com.k2.acs.model.FactorCalculator;
import com.k2.acs.model.CashFlow;
import com.k2.acs.model.ExposureMatrix;

import java.time.LocalDate;
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
            String configFilePath = args[0];
            AmsConfig config = AmsConfig.parseConfig(configFilePath);

            Pattern pattern = createPattern(config);

            UltimateValue ultimateValue = new UltimateValue(UltimateValue.Type.PREMIUM, config.getAmount());
            ultimateValue.addProperty("TOA", config.getToa());

            FactorCalculator factorCalculator = new FactorCalculator(config.getPrecision(), pattern);
            FactorCalculator.setUseCalendar(config.isCalendar());

            factorCalculator.calculateDailyFactors(config.getInsuredPeriodStartDateAsLocalDate(), FactorCalculator.FactorType.valueOf(config.getFactorType().toUpperCase()));
            factorCalculator.applyUltimateValueToPattern(ultimateValue);

            List<LocalDate> endPoints = ExposureMatrix.getEndDatesBetween(
                config.getCashFlowStartAsLocalDate().getYear(),
                config.getCashFlowEndAsLocalDate().getYear(),
                PatternElement.Type.valueOf(config.getCashFlowFrequency().toUpperCase())
            );

            List<CashFlow> cashFlows = factorCalculator.generateCashFlows(config.getInsuredPeriodStartDateAsLocalDate(), endPoints, config.isEndOfPeriod());
            processCashFlows(config, cashFlows);

    
            ExposureMatrix exposureMatrix = new ExposureMatrix(factorCalculator.getAllFactors(), config.getCashFlowStartAsLocalDate(), endPoints, endPoints, config.getPrecision(), config.isEndOfPeriod());
            
            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("\n" + exposureMatrix.generateExposureMatrixTable());
                getLogger().info("\n" + exposureMatrix.summarizeExposureMatrix());
            }   
        } catch (Exception e) {
            getLogger().warning("Error processing the configuration file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Pattern createPattern(AmsConfig config) {
        Pattern pattern = new Pattern();
        pattern.setType(config.getPatternType());
        pattern.setDuration(config.getDuration());
        for (AmsConfig.Element element : config.getElements()) {
            PatternElement patternElement = new PatternElement(
                element.getInitial(),
                element.getDistribution(),
                PatternElement.Type.valueOf(element.getType())
            );
            pattern.addElement(patternElement);
        }
        return pattern;
    }

    private static void processCashFlows(AmsConfig config, List<CashFlow> cashFlows) {
        LocalDate lbd = config.getLbdAsLocalDate();
        double sumBeforeLbd = 0.0;
        double sumAfterLbd = 0.0;
        for (CashFlow cashFlow : cashFlows) {
            cashFlow.setCurrency(config.getCurrency());
            cashFlow.addProperty("PATTERN_TYPE", config.getFactorType());
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
        bestEstimateCashFlow.addProperty("Factor", config.getFactorType());
        bestEstimateCashFlow.loadCashFlows(cashFlows);
        bestEstimateCashFlow.sortCashFlows();
        if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
            getLogger().info(bestEstimateCashFlow.toString());
            getLogger().info("Sum of cash flows before LBD: " + sumBeforeLbd);
            getLogger().info("Sum of cash flows after LBD: " + sumAfterLbd);
        }
    }
}
