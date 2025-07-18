package com.k2.acs;

import com.k2.acs.model.*;
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
            FileHandler fileHandler = new FileHandler("acs.log", false); // overwrite log file each run
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setUseParentHandlers(false); // stop log writing to console
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

            FactorCalculator factorCalculator = new FactorCalculator(config.getPrecision(), pattern);
            factorCalculator.setUseCalendar(config.isCalendar());
            factorCalculator.setUseLinear(config.isLinear());
            factorCalculator.setWrittenDate(config.getValuationDateAsLocalDate());

            factorCalculator.generateDailyFactors(
                    config.getInsuredPeriodStartDateAsLocalDate(),
                    FactorCalculator.FactorType.valueOf(config.getFactorType().toUpperCase())
            );

            List<LocalDate> financialPeriodsExposure = ExposureMatrix.getEndDatesBetween(
                    factorCalculator.getEarliestExposureDate().getYear(),
                    factorCalculator.getLatestExposureDate().getYear(),
                    PatternElement.Type.valueOf(config.getExposedTimeUnit().toUpperCase())
            );

            List<LocalDate> developmentPeriodsExposure = ExposureMatrix.getBucketEndDates(
                    config.getInsuredPeriodStartDateAsLocalDate(),
                    factorCalculator.getLatestExposureDate().getYear(),
                    PatternElement.Type.valueOf(config.getExposedTimeUnit().toUpperCase())
            );

            List<LocalDate> financialPeriodsIncurred = ExposureMatrix.getEndDatesBetween(
                    factorCalculator.getEarliestExposureDate().getYear(),
                    factorCalculator.getLatestExposureDate().getYear(),
                    PatternElement.Type.valueOf(config.getIncurredTimeUnit().toUpperCase())
            );

            List<LocalDate> developmentPeriodsIncurred = ExposureMatrix.getBucketEndDates(
                    config.getInsuredPeriodStartDateAsLocalDate(),
                    factorCalculator.getLatestIncurredDate().getYear(),
                    PatternElement.Type.valueOf(config.getIncurredTimeUnit().toUpperCase())
            );


            ExposureMatrix developmentMatrix = new ExposureMatrix(
                    factorCalculator.getAllFactors(),
                    config.getInsuredPeriodStartDateAsLocalDate(),
                    developmentPeriodsIncurred,
                    developmentPeriodsExposure,
                    config.isEndOfPeriod()
            );

            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("Development Period Factor Matrix");
                getLogger().info("\n" + developmentMatrix.generateExposureMatrixTable(config.getPrecision()));
            }

            ExposureMatrix financialMatrix = new ExposureMatrix(
                    factorCalculator.getAllFactors(),
                    config.getInsuredPeriodStartDateAsLocalDate(),
                    financialPeriodsIncurred,
                    financialPeriodsExposure,
                    config.isEndOfPeriod()
            );

            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("Financial Period Factor Matrix");
                getLogger().info("\n" + financialMatrix.generateExposureMatrixTable(config.getPrecision()));
            }

            ExposureMatrix standardMatrix = new ExposureMatrix(
                    factorCalculator.getAllFactors(),
                    config.getInsuredPeriodStartDateAsLocalDate(),
                    developmentPeriodsIncurred,
                    financialPeriodsExposure,
                    config.isEndOfPeriod()
            );

            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("Standard View Factor Matrix");
                getLogger().info("\n" + standardMatrix.generateExposureMatrixTable(config.getPrecision()));
            }

            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("Incurred Factor Vector (Financial Periods)");
                getLogger().info("\n" + printExposureVector(
                    financialMatrix.generateExposureVector(false), config.getPrecision()));
            }

            if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                getLogger().info("Earned Factor Vector (Financial Periods)");
                getLogger().info("\n" + printExposureVector(
                    financialMatrix.generateExposureVector(), config.getPrecision()));
            }

            for (UltimateValue uv : ultimateValues) {
                ExposureMatrix exposureMatrix = new ExposureMatrix(
                        factorCalculator.applyUltimateValueToPattern(uv),
                        config.getInsuredPeriodStartDateAsLocalDate(),
                        developmentPeriodsIncurred,
                        financialPeriodsExposure,
                        config.isEndOfPeriod()
                );

                if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                    getLogger().info("Applying UltimateValue of type: " + uv.getType() + " with amount: " + uv.getAmount());
                    getLogger().info("\n" + exposureMatrix.generateExposureMatrixTable(2));
                }
            }

           logger.setUseParentHandlers(true);
           String outputString = args[1];
            if (outputString != null && !outputString.isEmpty()) {
                if (!isValidOutputString(outputString)) {
                    getLogger().warning("Invalid output string format. Must be exactly 4 characters: " +
                            "1st character: E or W, 2nd and 3rd characters: F or D, 4th character: C, R, or M");
                    return;
                }

                // Determine FactorType based on first character of outputString
                FactorCalculator.FactorType factorType = outputString.toUpperCase().charAt(0) == 'E' 
                    ? FactorCalculator.FactorType.EARNING 
                    : FactorCalculator.FactorType.WRITING;

                factorCalculator.generateDailyFactors(
                        config.getInsuredPeriodStartDateAsLocalDate(),
                        factorType
                );

                // Determine periods for ExposureMatrix based on outputString
                List<LocalDate> incurredPeriods;
                List<LocalDate> exposurePeriods;
                char secondChar = Character.toUpperCase(outputString.charAt(1));
                char thirdChar = Character.toUpperCase(outputString.charAt(2));

                if (secondChar == 'D') {
                    incurredPeriods = developmentPeriodsIncurred;
                } else {
                    incurredPeriods = financialPeriodsIncurred;
                }

                if (thirdChar == 'D') {
                    exposurePeriods = developmentPeriodsExposure;
                } else {
                    exposurePeriods = financialPeriodsExposure;
                }

                ExposureMatrix outputMatrix = new ExposureMatrix(
                        factorCalculator.getAllFactors(),
                        config.getInsuredPeriodStartDateAsLocalDate(),
                        incurredPeriods,
                        exposurePeriods,
                        config.isEndOfPeriod()
                );

                char fourthChar = Character.toUpperCase(outputString.charAt(3));
                if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                    if (fourthChar == 'M') {
                        getLogger().info("Output ExposureMatrix (Matrix View):");
                        getLogger().info("\n" + outputMatrix.generateExposureMatrixTable(config.getPrecision()));
                    } else if (fourthChar == 'C') {
                        getLogger().info("Output ExposureMatrix (Cumulative Columns):");
                        getLogger().info("\n" + printExposureVector(outputMatrix.generateExposureVector(), config.getPrecision()));
                    } else if (fourthChar == 'R') {
                        getLogger().info("Output ExposureMatrix (Cumulative Rows):");
                        getLogger().info("\n" + printExposureVector(outputMatrix.generateExposureVector(false), config.getPrecision()));
                    }
                }
                
            }
            
        } catch (Exception e) {
            getLogger().warning("Error processing the configuration file: " + e.getMessage());
        }
    }

     private static boolean isValidOutputString(String outputString) {
        if (outputString == null || outputString.length() != 4) {
            return false;
        }
        
        String upper = outputString.toUpperCase();
        
        // First character must be E or W
        char first = upper.charAt(0);
        if (first != 'E' && first != 'W') {
            return false;
        }
        
        // Second and third characters must be F or D
        char second = upper.charAt(1);
        char third = upper.charAt(2);
        if ((second != 'F' && second != 'D') || (third != 'F' && third != 'D')) {
            return false;
        }
        
        // Fourth character must be C, R, or M
        char fourth = upper.charAt(3);
        if (fourth != 'C' && fourth != 'R' && fourth != 'M') {
            return false;
        }
        
        return true;
    }

    private static Pattern createPattern(AmsConfig config) {
        Pattern pattern = new Pattern();
        for (AmsConfig.Element element : config.getElements()) {
            PatternElement patternElement = new PatternElement(
                    element.getInitial(),
                    element.getDistribution(),
                    PatternElement.Type.valueOf(element.getType().toUpperCase()),
                    element.getInitialDuration() > 0 ? element.getInitialDuration() : config.getDefaultDuration(),
                    element.getDuration() > 0 ? element.getDuration() : config.getDefaultDuration());
            pattern.addElement(patternElement);
        }
        return pattern;
    }

    private static void printFactorsTable(List<Factor> factors) {
        StringBuilder table = new StringBuilder();
        table.append(String.format("%-15s %-15s %-15s", "Incurred Date", "Exposure Date", "Factor"));
        table.append(String.format("%-15s %-15s %-15s", "-------------", "-------------", "------"));
        for (Factor factor : factors) {
            table.append(String.format("%-15s %-15s %-15.6f",
                    factor.getIncurredDate(),
                    factor.getExposureDate(),
                    factor.getValue()));
        }
        getLogger().info("\n" + table);
    }

    /**
     * Pretty prints a list of ExposureVectorEntry as a table.
     * @param vector List of ExposureVectorEntry
     * @param precision Number of decimal places to show for the sum
     * @return String table representation
     */
    public static String printExposureVector(List<ExposureMatrix.ExposureVectorEntry> vector, int precision) {
        StringBuilder sb = new StringBuilder();
        String formatHeader = "%-15s %-15s%n";
        String formatRow = "%-15s %-" + (10 + precision) + "." + precision + "f%n";
        sb.append(String.format(formatHeader, "Date Bucket", "Sum"));
        sb.append(String.format(formatHeader, "-----------", "---"));
        for (var entry : vector) {
            sb.append(String.format(formatRow, entry.getDateBucket(), entry.getSum()));
        }
        return sb.toString();
    }
}
