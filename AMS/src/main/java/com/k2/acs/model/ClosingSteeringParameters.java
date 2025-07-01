package com.k2.acs.model;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class ClosingSteeringParameters {
    public static List<SteeringParameter> convertQuartersToMonths(List<SteeringParameter> steeringParameters) {
        List<SteeringParameter> newSteeringParameters = new ArrayList<>();

        for (SteeringParameter steeringParameter : steeringParameters) {
            if (steeringParameter.getCspType() == 'Q') {
                int mCounter = 1;
                for (int i = 0; i < 3; i++) {
                    newSteeringParameters.add(new SteeringParameter('M',
                        (((steeringParameter.getCspIndex()-1) * 3) + mCounter++),
                        steeringParameter.getFactor()/3, 
                        steeringParameter.getDuration()));
                }
            } else {
                newSteeringParameters.add(steeringParameter);
            }
        }

        steeringParameters.clear();
        steeringParameters.addAll(newSteeringParameters);
        steeringParameters.sort((u1, u2) -> Integer.compare(u1.getCspIndex(), u2.getCspIndex())); // Sort by cspIndex
        return steeringParameters;
    }

    public static List<SteeringParameter> convertMonthsToQuarters(List<SteeringParameter> steeringParameters) {
        List<SteeringParameter> newSteeringParameters = new ArrayList<>();
        Map<Integer, List<SteeringParameter>> groupedByQuarter = new HashMap<>();

        for (SteeringParameter steeringParameter : steeringParameters) {
            if (steeringParameter.getCspType() == 'M') {
                int quarterIndex = (steeringParameter.getCspIndex() - 1) / 3;
                groupedByQuarter
                    .computeIfAbsent(quarterIndex, k -> new ArrayList<>())
                    .add(steeringParameter);
            } else {
                newSteeringParameters.add(steeringParameter);
            }
        }

        for (Map.Entry<Integer, List<SteeringParameter>> entry : groupedByQuarter.entrySet()) {
            List<SteeringParameter> monthSteeringParameters = entry.getValue();
            if (monthSteeringParameters.size() != 3) {
                throw new IllegalArgumentException("Cannot convert months to quarters: incomplete quarter data.");
            }
            double totalFactor = monthSteeringParameters.stream().mapToDouble(SteeringParameter::getFactor).sum();
            newSteeringParameters.add(new SteeringParameter('Q', 
                                                    entry.getKey() + 1,
                                                    totalFactor,
                                                    monthSteeringParameters.get(0).getDuration()));
        }

        steeringParameters.clear();
        steeringParameters.addAll(newSteeringParameters);
        steeringParameters.sort((u1, u2) -> Integer.compare(u1.getCspIndex(), u2.getCspIndex())); // Sort by cspIndex
        return steeringParameters;
    }

    private List<SteeringParameter> steeringParameters;
    private String name;
    private boolean scenario;
    private String scenarioType;

    public void parseFromCsvFile(String csvFilePath) throws CSPException {
        try (FileInputStream csvInputStream = new FileInputStream(csvFilePath)) {
            parseUnitsFromStream(csvInputStream, true, ",");
        } catch (Exception e) {
            throw new CSPException("Error reading CSV: " + e.getMessage(), e);
        }
    }

    public void parseUnitsFromStream(InputStream inputStream, boolean hasHeaderLine, String delimiter) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            steeringParameters = reader.lines()
                                .skip(hasHeaderLine ? 1 : 0)
                                .map(line -> {
                                    String[] parts = line.split(delimiter);
                                    if (parts.length != 4) {
                                        throw new IllegalArgumentException("Invalid line format: " + line);
                                    }
                                    char unitType = parts[0].charAt(0);
                                    int unitIndex = Integer.parseInt(parts[1]);
                                    double factor = Double.parseDouble(parts[2]);
                                    int duration = Integer.parseInt(parts[3]);
                                    return new SteeringParameter(unitType, unitIndex, factor, duration);
                                })
                                .toList();
            validate();
        }
    }

    public void validate() {
        boolean hasQ = steeringParameters.stream().anyMatch(steeringParameter -> steeringParameter.getCspType() == 'Q');
        boolean hasM = steeringParameters.stream().anyMatch(steeringParameter -> steeringParameter.getCspType() == 'M');
        boolean hasD = steeringParameters.stream().anyMatch(steeringParameter -> steeringParameter.getCspType() == 'D');

        if (hasQ && (hasM || hasD)) {
            validateQClosingSteeringParameters(hasM);
        }

        if (hasM) {
            validateMClosingSteeringParameters();
        }
    }

    private void validateQClosingSteeringParameters(boolean hasM) {
        if (hasM) {
            throw new IllegalArgumentException("Invalid cannot mix 'M' and 'Q' ClosingSteeringParameters.");
        }
        for (SteeringParameter steeringParameter : steeringParameters) {
            if (steeringParameter.getCspType() == 'D' && (steeringParameter.getCspIndex() - 1) % 90 != 0) {
                throw new IllegalArgumentException("Invalid cspIndex for 'D' when 'Q' is present.");
            }
        }
    }

    private void validateMClosingSteeringParameters() {
        for (SteeringParameter steeringParameter : steeringParameters) {
            if (steeringParameter.getCspType() == 'D' && (steeringParameter.getCspIndex() - 1) % 30 != 0) {
                throw new IllegalArgumentException("Invalid cspIndex for 'D' when 'M' is present.");
            }
        }
    }

    public List<PatternElement> toPatternElements() {
        Map<Integer, SteeringParameter> startPointMap = new HashMap<>();
        steeringParameters.stream()
            .filter(sp -> sp.getCspType() != 'D')
            .forEach(sp -> startPointMap.put(sp.getStartPoint(), sp));

        return steeringParameters.stream()
                .filter(sp -> sp.getCspType() != 'D')
                .map(sp -> {
                    PatternElement.Type type;
                    if (sp.getCspType() == 'Y') {
                        type = PatternElement.Type.YEAR;
                    } else if (sp.getCspType() == 'Q') {
                        type = PatternElement.Type.QUARTER;
                    } else if (sp.getCspType() == 'M') {
                        type = PatternElement.Type.MONTH;
                    } else if (sp.getCspType() == 'W') {
                        type = PatternElement.Type.WEEK;
                    } else {
                        type = PatternElement.Type.DAY;
                    }

                    double initialDistribution = 0;
                    SteeringParameter matchingD = steeringParameters.stream()
                            .filter(d -> d.getCspType() == 'D' && d.getStartPoint() == sp.getStartPoint())
                            .findFirst()
                            .orElse(null);

                    if (matchingD != null) {
                        initialDistribution = matchingD.getFactor();
                    }

                    return new PatternElement(
                        initialDistribution,
                        sp.getFactor(),
                        type
                    );
                })
                .toList();
    }

    public static class CSPException extends Exception {
        public CSPException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
