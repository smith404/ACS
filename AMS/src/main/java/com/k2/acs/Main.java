package com.k2.acs;

import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.Pattern;
import com.k2.acs.model.PatternElement;
import com.k2.acs.model.BestEstimateCashFlow;
import com.k2.acs.model.FactorCalculator;
import com.k2.acs.model.CashFlow;
import com.k2.acs.model.ExposureMatrix;
import com.k2.acs.model.Factor;
import lombok.Getter;

import java.time.LocalDate;
import java.util.List;

import java.util.logging.Logger;

public class Main {
    @Getter
    private static final Logger logger = Logger.getLogger(Main.class.getName());

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

            FactorCalculator factorCalculator = new FactorCalculator(config.getPrecision(), pattern, config.getRiskAttachingDuration());
            FactorCalculator.setUseCalendar(config.isCalendar());

            factorCalculator.calculateDailyFactors(config.getInsuredPeriodStartDateAsLocalDate(), FactorCalculator.FactorType.valueOf(config.getFactorType().toUpperCase()));

            factorCalculator.applyUltimateValueToPattern(ultimateValue);

            List<LocalDate> endPoints = ExposureMatrix.getEndDatesBetween(
                factorCalculator.getEarliestExposureDate().getYear(),
                factorCalculator.getLatestExposureDate().getYear(),
                PatternElement.Type.valueOf(config.getExposedTimeUnit().toUpperCase())
            );

            //List<CashFlow> cashFlows = factorCalculator.generateCashFlows(config.getInsuredPeriodStartDateAsLocalDate(), endPoints, config.isEndOfPeriod());
            //processCashFlows(config, cashFlows);
    
            // Get all factors and print them in a tabulated format
            List<Factor> allFactors = factorCalculator.getAllFactors();
            printFactorsTable(allFactors);
    
            ExposureMatrix exposureMatrix = new ExposureMatrix(factorCalculator.getAllFactors(), config.getInsuredPeriodStartDateAsLocalDate(), endPoints, endPoints, config.getPrecision(), config.isEndOfPeriod());
            

            
            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("\n" + exposureMatrix.generateExposureMatrixTable());
                getLogger().info("\n" + exposureMatrix.getExposureBuckets());
                getLogger().info("\n" + exposureMatrix.getIncurredBuckets());
            }   
        } catch (Exception e) {
            getLogger().warning("Error processing the configuration file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Pattern createPattern(AmsConfig config) {
        Pattern pattern = new Pattern();
        for (AmsConfig.Element element : config.getElements()) {
            PatternElement patternElement = new PatternElement(
                element.getInitial(),
                element.getDistribution(),
                PatternElement.Type.valueOf(element.getType().toUpperCase())
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
        if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
            getLogger().info(bestEstimateCashFlow.toString());
            getLogger().info("Sum of cash flows before LBD: " + sumBeforeLbd);
            getLogger().info("Sum of cash flows after LBD: " + sumAfterLbd);
        }
    }

    private static void printFactorsTable(List<Factor> factors) {
        StringBuilder table = new StringBuilder();
        table.append(String.format("%-15s %-15s %-15s %-15s %-15s%n", "Incurred Date", "Distribution", "Exposure Date", "Dev Period", "Value"));
        table.append(String.format("%-15s %-15s %-15s %-15s %-15s%n", "-------------", "------------", "-------------", "----------", "-----"));
        for (Factor factor : factors) {
            table.append(String.format("%-15s %-15.6f %-15s %-15.6f%n", 
                factor.getIncurredDate(), 
                factor.getDistribution(), 
                factor.getExposureDate(), 
                factor.getValue()));
        }
        getLogger().info("\n" + table.toString());
    }
}
