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
        Arguments arguments = parseArguments(args);
        if (arguments == null) {
            return;
        }

        try {
            String configFilePath = arguments.configFile;
            if (!configFilePath.endsWith(".json")) {
                configFilePath += ".json";
            }
            
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

            logger.setUseParentHandlers(true);

            if (arguments.resultString != null && !arguments.resultString.isEmpty()) {
                if (!isValidOutputString(arguments.resultString)) {
                    getLogger().warning("Invalid output string format. Must be exactly 4 characters: " +
                            "1st character: E or W, 2nd and 3rd characters: F or D, 4th character: C, R, or M");
                    return;
                }

                // Determine FactorType based on first character of outputString
                FactorCalculator.FactorType factorType = arguments.resultString.toUpperCase().charAt(0) == 'E' 
                    ? FactorCalculator.FactorType.EARNING 
                    : FactorCalculator.FactorType.WRITING;

                FactorCalculator factorCalculator = new FactorCalculator(config.getPrecision(), pattern);
                factorCalculator.setUseCalendar(config.isCalendar());
                factorCalculator.setUseLinear(config.isLinear());
                factorCalculator.setFast(config.isFast());
                factorCalculator.setWrittenDate(config.getValuationDateAsLocalDate());

                factorCalculator.generateDailyFactors(
                        config.getInsuredPeriodStartDateAsLocalDate(),
                        factorType
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

                // Determine periods for ExposureMatrix based on outputString
                List<LocalDate> incurredPeriods;
                List<LocalDate> exposurePeriods;
                char secondChar = Character.toUpperCase(arguments.resultString.charAt(1));
                char thirdChar = Character.toUpperCase(arguments.resultString.charAt(2));

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

                char fourthChar = Character.toUpperCase(arguments.resultString.charAt(3));
                if (getLogger().isLoggable(java.util.logging.Level.INFO)) {
                    if (fourthChar == 'M') {
                        getLogger().info("Output ExposureMatrix (Matrix View):");
                        getLogger().info("\n" + outputMatrix.generateExposureMatrixTable(config.getPrecision(), arguments.csv));
                    } else if (fourthChar == 'C') {
                        getLogger().info("Output ExposureMatrix (Cumulative Columns):");
                        getLogger().info("\n" + printExposureVector(outputMatrix.generateExposureVector(), config.getPrecision(), arguments.csv));
                    } else if (fourthChar == 'R') {
                        getLogger().info("Output ExposureMatrix (Cumulative Rows):");
                        getLogger().info("\n" + printExposureVector(outputMatrix.generateExposureVector(false), config.getPrecision(), arguments.csv));
                    }
                }
            }
            
        } catch (Exception e) {
            getLogger().warning("Error processing the configuration file: " + e.getMessage());
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static class Arguments {
        String configFile;
        String resultString;
        boolean csv;
    }

    private static Arguments parseArguments(String[] args) {
        if (args.length == 0) {
            getLogger().warning("Usage: -config <config_file> [-result <result_string>] [-csv]");
            return null;
        }

        Arguments arguments = new Arguments();
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-config":
                    if (i + 1 < args.length) {
                        arguments.configFile = args[++i];
                    } else {
                        getLogger().warning("Please provide a value for -config parameter.");
                        return null;
                    }
                    break;
                case "-result":
                    if (i + 1 < args.length) {
                        arguments.resultString = args[++i];
                    } else {
                        getLogger().warning("Please provide a value for -result parameter.");
                        return null;
                    }
                    break;
                case "-csv":
                    arguments.csv = true;
                    break;
                default:
                    getLogger().warning("Unknown parameter: " + args[i]);
                    return null;
            }
        }

        if (arguments.configFile == null) {
            getLogger().warning("Please provide the -config parameter with the path to the JSON configuration file.");
            return null;
        }

        return arguments;
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
        printFactorsTable(factors, false);
    }

    /**
     * Prints a table of factors. If csv is true, outputs as comma-separated values.
     * @param factors List of Factor
     * @param csv If true, outputs as CSV
     */
    private static void printFactorsTable(List<Factor> factors, boolean csv) {
        StringBuilder table = new StringBuilder();
        if (csv) {
            table.append(String.format("Incurred Date,Exposure Date,Factor%n"));
            for (Factor factor : factors) {
                table.append(String.format("%s,%s,%.6f%n",
                        factor.getIncurredDate(),
                        factor.getExposureDate(),
                        factor.getValue()));
            }
        } else {
            table.append(String.format("%-15s %-15s %-15s%n", "Incurred Date", "Exposure Date", "Factor"));
            table.append(String.format("%-15s %-15s %-15s%n", "-------------", "-------------", "------"));
            for (Factor factor : factors) {
                table.append(String.format("%-15s %-15s %-15.6f%n",
                        factor.getIncurredDate(),
                        factor.getExposureDate(),
                        factor.getValue()));
            }
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
        return printExposureVector(vector, precision, false);
    }

    /**
     * Pretty prints a list of ExposureVectorEntry as a table or CSV.
     * @param vector List of ExposureVectorEntry
     * @param precision Number of decimal places to show for the sum
     * @param csv If true, outputs as CSV
     * @return String table or CSV representation
     */
    public static String printExposureVector(List<ExposureMatrix.ExposureVectorEntry> vector, int precision, boolean csv) {
        StringBuilder sb = new StringBuilder();
        if (csv) {
            sb.append("Date Bucket,Sum\n");
            for (var entry : vector) {
                sb.append(String.format("%s,%-" + (10 + precision) + "." + precision + "f%n", entry.getDateBucket(), entry.getSum()));
            }
        } else {
            String formatHeader = "%-15s %-15s%n";
            String formatRow = "%-15s %-" + (10 + precision) + "." + precision + "f%n";
            sb.append(String.format(formatHeader, "Date Bucket", "Sum"));
            sb.append(String.format(formatHeader, "-----------", "---"));
            for (var entry : vector) {
                sb.append(String.format(formatRow, entry.getDateBucket(), entry.getSum()));
            }
        }
        return sb.toString();
    }
}
