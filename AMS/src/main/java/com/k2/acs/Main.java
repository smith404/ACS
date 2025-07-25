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
    
    private static final String USAGE_MESSAGE = "Usage: -config <config_file> [-result <result_string>] [-csv]";
    private static final String VALID_OUTPUT_FORMAT = "Invalid output string format. Must be exactly 4 characters: " +
            "1st character: E or W, 2nd and 3rd characters: F, D, or C, 4th character: C, R, or M";

    static {
        try {
            FileHandler fileHandler = new FileHandler("acs.log", false);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setUseParentHandlers(false);
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
            processConfiguration(arguments);
        } catch (Exception e) {
            handleProcessingError(e);
        }
    }

    private static void processConfiguration(Arguments arguments) throws Exception {
        AmsConfig config = loadConfiguration(arguments.configFile);
        Pattern pattern = createPattern(config);
        List<UltimateValue> ultimateValues = createUltimateValues(config);

        logger.setUseParentHandlers(true);

        if (arguments.resultString != null && !arguments.resultString.isEmpty()) {
            processResultOutput(arguments, config, pattern);
        }
    }

    private static AmsConfig loadConfiguration(String configFile) throws Exception {
        String configFilePath = configFile.endsWith(".json") ? configFile : configFile + ".json";
        return AmsConfig.parseConfig(configFilePath);
    }

    private static List<UltimateValue> createUltimateValues(AmsConfig config) {
        List<UltimateValue> ultimateValues = new ArrayList<>();
        if (config.getUltimateValues() != null) {
            for (AmsConfig.UV uv : config.getUltimateValues()) {
                UltimateValue.Type type = UltimateValue.Type.valueOf(uv.getType().toUpperCase());
                double amount = uv.getValue() != null ? uv.getValue() : 1.0;
                ultimateValues.add(new UltimateValue(type, amount));
            }
        }
        return ultimateValues;
    }

    private static void processResultOutput(Arguments arguments, AmsConfig config, Pattern pattern) {
        if (!isValidOutputString(arguments.resultString)) {
            logger.warning(VALID_OUTPUT_FORMAT);
            return;
        }

        FactorCalculator factorCalculator = createFactorCalculator(arguments, config, pattern);
        PeriodConfiguration periodConfig = createPeriodConfiguration(arguments, config, factorCalculator);
        ExposureMatrix outputMatrix = createExposureMatrix(config, factorCalculator, periodConfig);

        logOutputMatrix(arguments.resultString, outputMatrix, config.getPrecision(), arguments.csv);
    }

    private static FactorCalculator createFactorCalculator(Arguments arguments, AmsConfig config, Pattern pattern) {
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

        return factorCalculator;
    }

    private static PeriodConfiguration createPeriodConfiguration(Arguments arguments, AmsConfig config, FactorCalculator factorCalculator) {
        PatternElement.Type exposureTimeUnit = PatternElement.Type.valueOf(config.getExposedTimeUnit().toUpperCase());
        PatternElement.Type incurredTimeUnit = PatternElement.Type.valueOf(config.getIncurredTimeUnit().toUpperCase());

        List<LocalDate> financialPeriodsExposure = ExposureMatrix.getEndDatesBetween(
                factorCalculator.getEarliestExposureDate().getYear(),
                factorCalculator.getLatestExposureDate().getYear(),
                exposureTimeUnit
        );

        List<LocalDate> combinedPeriodsExposure = createCombinedPeriods(
                financialPeriodsExposure,
                config.getInsuredPeriodStartDateAsLocalDate(),
                factorCalculator.getLatestExposureDate().getYear(),
                exposureTimeUnit
        );

        List<LocalDate> developmentPeriodsExposure = ExposureMatrix.getBucketEndDates(
                config.getInsuredPeriodStartDateAsLocalDate(),
                factorCalculator.getLatestExposureDate().getYear(),
                exposureTimeUnit
        );

        List<LocalDate> financialPeriodsIncurred = ExposureMatrix.getEndDatesBetween(
                factorCalculator.getEarliestExposureDate().getYear(),
                factorCalculator.getLatestExposureDate().getYear(),
                incurredTimeUnit
        );

        List<LocalDate> combinedPeriodsIncurred = createCombinedPeriods(
                financialPeriodsIncurred,
                config.getInsuredPeriodStartDateAsLocalDate(),
                factorCalculator.getLatestIncurredDate().getYear(),
                incurredTimeUnit
        );

        List<LocalDate> developmentPeriodsIncurred = ExposureMatrix.getBucketEndDates(
                config.getInsuredPeriodStartDateAsLocalDate(),
                factorCalculator.getLatestIncurredDate().getYear(),
                incurredTimeUnit
        );

        return new PeriodConfiguration(
                financialPeriodsExposure, combinedPeriodsExposure, developmentPeriodsExposure,
                financialPeriodsIncurred, combinedPeriodsIncurred, developmentPeriodsIncurred,
                arguments.resultString
        );
    }

    private static List<LocalDate> createCombinedPeriods(List<LocalDate> financialPeriods, 
                                                         LocalDate insuredPeriodStart, 
                                                         int latestYear, 
                                                         PatternElement.Type timeUnit) {
        List<LocalDate> combined = new ArrayList<>(financialPeriods);
        combined.addAll(ExposureMatrix.getBucketEndDates(insuredPeriodStart, latestYear, timeUnit));
        return combined.stream().distinct().sorted().toList();
    }

    private static ExposureMatrix createExposureMatrix(AmsConfig config, FactorCalculator factorCalculator, PeriodConfiguration periodConfig) {
        List<LocalDate> incurredPeriods = periodConfig.getIncurredPeriods();
        List<LocalDate> exposurePeriods = periodConfig.getExposurePeriods();

        //printFactorsTable(factorCalculator.getAllFactors(), true);

        return new ExposureMatrix(
                factorCalculator.getAllFactors(),
                config.getInsuredPeriodStartDateAsLocalDate(),
                incurredPeriods,
                exposurePeriods,
                config.isEndOfPeriod()
        );
    }

    private static void logOutputMatrix(String resultString, ExposureMatrix outputMatrix, int precision, boolean csv) {
        if (!logger.isLoggable(java.util.logging.Level.INFO)) {
            return;
        }

        char outputType = Character.toUpperCase(resultString.charAt(3));
        switch (outputType) {
            case 'M':
                logger.info("Output ExposureMatrix (Matrix View):");
                logger.info("\n" + outputMatrix.generateExposureMatrixTable(precision, csv));
                break;
            case 'C':
                logger.info("Output ExposureMatrix (Cumulative Columns):");
                logger.info("\n" + printExposureVector(outputMatrix.generateExposureVector(), precision, csv));
                break;
            case 'R':
                logger.info("Output ExposureMatrix (Cumulative Rows):");
                logger.info("\n" + printExposureVector(outputMatrix.generateExposureVector(false), precision, csv));
                break;
        }
    }

    private static void handleProcessingError(Exception e) {
        logger.warning("Error processing the configuration file: " + e.getMessage());
        System.err.println("Error: " + e.getMessage());
        e.printStackTrace();
    }

    private static class Arguments {
        String configFile;
        String resultString;
        boolean csv;
    }

    private static class PeriodConfiguration {
        private final List<LocalDate> financialPeriodsExposure;
        private final List<LocalDate> combinedPeriodsExposure;
        private final List<LocalDate> developmentPeriodsExposure;
        private final List<LocalDate> financialPeriodsIncurred;
        private final List<LocalDate> combinedPeriodsIncurred;
        private final List<LocalDate> developmentPeriodsIncurred;
        private final String resultString;

        public PeriodConfiguration(List<LocalDate> financialPeriodsExposure, List<LocalDate> combinedPeriodsExposure,
                                 List<LocalDate> developmentPeriodsExposure, List<LocalDate> financialPeriodsIncurred,
                                 List<LocalDate> combinedPeriodsIncurred, List<LocalDate> developmentPeriodsIncurred,
                                 String resultString) {
            this.financialPeriodsExposure = financialPeriodsExposure;
            this.combinedPeriodsExposure = combinedPeriodsExposure;
            this.developmentPeriodsExposure = developmentPeriodsExposure;
            this.financialPeriodsIncurred = financialPeriodsIncurred;
            this.combinedPeriodsIncurred = combinedPeriodsIncurred;
            this.developmentPeriodsIncurred = developmentPeriodsIncurred;
            this.resultString = resultString;
        }

        public List<LocalDate> getIncurredPeriods() {
            char secondChar = Character.toUpperCase(resultString.charAt(1));
            return switch (secondChar) {
                case 'C' -> combinedPeriodsIncurred;
                case 'D' -> developmentPeriodsIncurred;
                default -> financialPeriodsIncurred;
            };
        }

        public List<LocalDate> getExposurePeriods() {
            char thirdChar = Character.toUpperCase(resultString.charAt(2));
            return switch (thirdChar) {
                case 'C' -> combinedPeriodsExposure;
                case 'D' -> developmentPeriodsExposure;
                default -> financialPeriodsExposure;
            };

        }
    }

    private static Arguments parseArguments(String[] args) {
        if (args.length == 0) {
            logger.warning(USAGE_MESSAGE);
            return null;
        }

        Arguments arguments = new Arguments();
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-config":
                    if (i + 1 < args.length) {
                        arguments.configFile = args[++i];
                    } else {
                        logger.warning("Please provide a value for -config parameter.");
                        return null;
                    }
                    break;
                case "-result":
                    if (i + 1 < args.length) {
                        arguments.resultString = args[++i];
                    } else {
                        logger.warning("Please provide a value for -result parameter.");
                        return null;
                    }
                    break;
                case "-csv":
                    arguments.csv = true;
                    break;
                default:
                    logger.warning("Unknown parameter: " + args[i]);
                    return null;
            }
        }

        if (arguments.configFile == null) {
            logger.warning("Please provide the -config parameter with the path to the JSON configuration file.");
            return null;
        }

        return arguments;
    }

    /**
     * Validates the output string format.
     * Expected format: 4 characters where:
     * - 1st character: E (Earning) or W (Writing)
     * - 2nd character: F (Financial), D (Development), or C (Combined) for incurred periods
     * - 3rd character: F (Financial), D (Development), or C (Combined) for exposure periods  
     * - 4th character: C (Columns), R (Rows), or M (Matrix)
     */
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
        
        // Second and third characters must be F, D, or C
        char second = upper.charAt(1);
        char third = upper.charAt(2);
        if (!isValidPeriodChar(second) || !isValidPeriodChar(third)) {
            return false;
        }
        
        // Fourth character must be C, R, or M
        char fourth = upper.charAt(3);
        if (fourth != 'C' && fourth != 'R' && fourth != 'M') {
            return false;
        }
        
        return true;
    }

    private static boolean isValidPeriodChar(char c) {
        return c == 'F' || c == 'D' || c == 'C';
    }

    /**
     * Creates a pattern from the AMS configuration.
     * @param config AMS configuration
     * @return Pattern object
     */
    private static Pattern createPattern(AmsConfig config) {
        Pattern pattern = new Pattern();
        for (AmsConfig.Element element : config.getElements()) {
            PatternFactor patternElement = new PatternFactor(
                    PatternFactor.Type.valueOf(element.getType().toUpperCase()),
                    element.getInitial(),
                    element.getDistribution(),
                    element.getInitialDuration() > 0 ? element.getInitialDuration() : config.getDefaultDuration(),
                    element.getDuration() > 0 ? element.getDuration() : config.getDefaultDuration());
            pattern.addElement(patternElement);
        }
        return pattern;
    }

    /**
     * Prints a table of factors. If csv is true, outputs as comma-separated values.
     * @param factors List of Factor
     * @param csv If true, outputs as CSV
     */
    private static void printFactorsTable(List<Factor> factors, boolean csv) {
        // Sort factors by incurred date first, then by exposure date
        List<Factor> sortedFactors = factors.stream()
                .sorted((f1, f2) -> {
                    int incurredComparison = f1.getIncurredDate().compareTo(f2.getIncurredDate());
                    if (incurredComparison != 0) {
                        return incurredComparison;
                    }
                    return f1.getExposureDate().compareTo(f2.getExposureDate());
                })
                .toList();
        
        StringBuilder table = new StringBuilder();
        if (csv) {
            table.append(String.format("Incurred Date,Exposure Date,Factor,Factor Type%n"));
            for (Factor factor : sortedFactors) {
                table.append(String.format("%s,%s,%.6f,%s%n",
                        factor.getIncurredDate(),
                        factor.getExposureDate(),
                        factor.getValue(),
                        factor.getFactorType() // assumes getFactorType() exists
                ));
            }
        } else {
            table.append(String.format("%-15s %-15s %-15s %-15s%n", "Incurred Date", "Exposure Date", "Factor", "Factor Type"));
            table.append(String.format("%-15s %-15s %-15s %-15s%n", "-------------", "-------------", "------", "-----------"));
            for (Factor factor : sortedFactors) {
                table.append(String.format("%-15s %-15s %-15.6f %-15s%n",
                        factor.getIncurredDate(),
                        factor.getExposureDate(),
                        factor.getValue(),
                        factor.getFactorType() // assumes getFactorType() exists
                ));
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
