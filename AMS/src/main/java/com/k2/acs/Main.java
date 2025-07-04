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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Main {
    @Getter
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    static {
        try {
            FileHandler fileHandler = new FileHandler("acs.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setUseParentHandlers(true);
        } catch (Exception e) {
            System.err.println("Failed to set up file logging: " + e.getMessage());
        }
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

            List<UltimateValue> ultimateValues = new ArrayList<>();
            if (config.getUltimateValues() != null) {
                for (AmsConfig.UV uv : config.getUltimateValues()) {
                    UltimateValue.Type type = UltimateValue.Type.valueOf(uv.getType().toUpperCase());
                    double amount = uv.getValue() != null ? uv.getValue() : 1.0;
                    ultimateValues.add(new UltimateValue(type, amount));
                }
            }

            int riskAttachingDuration = config.getRiskAttachingDuration();

            FactorCalculator factorCalculator = new FactorCalculator(config.getPrecision(), pattern);
            factorCalculator.setUseCalendar(config.isCalendar());
            factorCalculator.setWrittenDate(config.getValuationDateAsLocalDate());

            factorCalculator.calculateDailyFactors(
                config.getInsuredPeriodStartDateAsLocalDate(),
                FactorCalculator.FactorType.valueOf(config.getFactorType().toUpperCase())
            );

            List<LocalDate> endPoints = ExposureMatrix.getEndDatesBetween(
                factorCalculator.getEarliestExposureDate().getYear(),
                factorCalculator.getLatestExposureDate().getYear(),
                PatternElement.Type.valueOf(config.getExposedTimeUnit().toUpperCase())
            );

            ExposureMatrix exposureMatrix = new ExposureMatrix(
                factorCalculator.getAllFactors(),
                config.getInsuredPeriodStartDateAsLocalDate(),
                endPoints,
                endPoints,
                config.getPrecision(),
                config.isEndOfPeriod()
            );

            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("Factor Matrix");
                getLogger().info("\n" + exposureMatrix.generateExposureMatrixTable());
            }

            for (UltimateValue uv : ultimateValues) {
                factorCalculator.applyUltimateValueToPattern(uv);

                exposureMatrix = new ExposureMatrix(
                    factorCalculator.getAllFactors(),
                    config.getInsuredPeriodStartDateAsLocalDate(),
                    endPoints,
                    endPoints,
                    config.getPrecision(),
                    config.isEndOfPeriod()
                );

                if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                    getLogger().info("Applying UltimateValue of type: " + uv.getType() + " with amount: " + uv.getAmount());
                    getLogger().info("\n" + exposureMatrix.generateExposureMatrixTable());
                }

                List<Factor> allFactors = factorCalculator.getAllFactors();
                printFactorsTable(allFactors);
            }

        } catch (Exception e) {
            getLogger().warning("Error processing the configuration file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Pattern createPattern(AmsConfig config) {
        Pattern pattern = new Pattern();
        for (AmsConfig.Element element : config.getElements()) {
            if (element.getRiskAttachingDuration() < 0) {
                element.setRiskAttachingDuration(config.getRiskAttachingDuration());
            }
            PatternElement patternElement = new PatternElement(
                element.getInitial(),
                element.getDistribution(),
                PatternElement.Type.valueOf(element.getType().toUpperCase()),
                element.getRiskAttachingDuration());
            pattern.addElement(patternElement);
        }
        return pattern;
    }
    
    private static void printFactorsTable(List<Factor> factors) {
        StringBuilder table = new StringBuilder();
        table.append(String.format("%-15s %-15s %-15s %-15s %-10s%n", "Incurred Date", "Exposure Date", "Distribution", "Value", "isWritten"));
        table.append(String.format("%-15s %-15s %-15s %-15s %-10s%n", "-------------", "-------------", "------------", "-----", "---------"));
        for (Factor factor : factors) {
            table.append(String.format("%-15s %-15s %-15.6f %-15.6f %-10s%n", 
                factor.getIncurredDate(), 
                factor.getExposureDate(), 
                factor.getDistribution(), 
                factor.getValue(),
                factor.isWritten()));
        }
        getLogger().info("\n" + table.toString());
    }
}
