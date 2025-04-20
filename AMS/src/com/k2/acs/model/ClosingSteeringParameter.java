package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.k2.acs.model.PatternElement.Type;

@Data
@NoArgsConstructor
public class ClosingSteeringParameter {

    public static List<ClosingSteeringParameter> parseUnitsFromStream(InputStream inputStream, boolean hasHeaderLine, String delimiter) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            return reader.lines()
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
                             return new ClosingSteeringParameter(unitType, unitIndex, factor, duration);
                         })
                         .toList();
        }
    }

    public static List<ClosingSteeringParameter> parseFromJsonStream(InputStream inputStream) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(inputStream, new TypeReference<List<ClosingSteeringParameter>>() {});
    }

    public static void validate(List<ClosingSteeringParameter> closingSteeringParameters) {
        boolean hasQ = closingSteeringParameters.stream().anyMatch(closingSteeringParameter -> closingSteeringParameter.getCspType() == 'Q');
        boolean hasM = closingSteeringParameters.stream().anyMatch(closingSteeringParameter -> closingSteeringParameter.getCspType() == 'M');
        boolean hasD = closingSteeringParameters.stream().anyMatch(closingSteeringParameter -> closingSteeringParameter.getCspType() == 'D');

        if (hasQ && (hasM || hasD)) {
            validateQClosingSteeringParameters(closingSteeringParameters, hasM);
        }

        if (hasM) {
            validateMClosingSteeringParameters(closingSteeringParameters);
        }
    }

    private static void validateQClosingSteeringParameters(List<ClosingSteeringParameter> closingSteeringParameters, boolean hasM) {
        if (hasM) {
            throw new IllegalArgumentException("Invalid cannot mix 'M' and 'Q' ClosingSteeringParameters.");
        }
        for (ClosingSteeringParameter closingSteeringParameter : closingSteeringParameters) {
            if (closingSteeringParameter.getCspType() == 'D' && (closingSteeringParameter.getCspIndex() - 1) % 90 != 0) {
                throw new IllegalArgumentException("Invalid cspIndex for 'D' when 'Q' is present.");
            }
        }
    }

    private static void validateMClosingSteeringParameters(List<ClosingSteeringParameter> closingSteeringParameters) {
        for (ClosingSteeringParameter closingSteeringParameter : closingSteeringParameters) {
            if (closingSteeringParameter.getCspType() == 'D' && (closingSteeringParameter.getCspIndex() - 1) % 30 != 0) {
                throw new IllegalArgumentException("Invalid cspIndex for 'D' when 'M' is present.");
            }
        }
    }

    public static List<ClosingSteeringParameter> convertQuartersToMonths(List<ClosingSteeringParameter> closingSteeringParameters) {
        List<ClosingSteeringParameter> newClosingSteeringParameters = new ArrayList<>();

        for (ClosingSteeringParameter closingSteeringParameter : closingSteeringParameters) {
            if (closingSteeringParameter.getCspType() == 'Q') {
                int mCounter = 1;
                for (int i = 0; i < 3; i++) {
                    newClosingSteeringParameters.add(new ClosingSteeringParameter('M',
                        (((closingSteeringParameter.cspIndex-1) * 3) + mCounter++),
                        closingSteeringParameter.getFactor()/3, 
                        closingSteeringParameter.getDuration()));
                }
            } else {
                newClosingSteeringParameters.add(closingSteeringParameter);
            }
        }

        closingSteeringParameters.clear();
        closingSteeringParameters.addAll(newClosingSteeringParameters);
        closingSteeringParameters.sort((u1, u2) -> Integer.compare(u1.getCspIndex(), u2.getCspIndex())); // Sort by cspIndex
        return closingSteeringParameters;
    }

    public static List<ClosingSteeringParameter> convertMonthsToQuarters(List<ClosingSteeringParameter> closingSteeringParameters) {
        List<ClosingSteeringParameter> newClosingSteeringParameters = new ArrayList<>();
        Map<Integer, List<ClosingSteeringParameter>> groupedByQuarter = new HashMap<>();

        for (ClosingSteeringParameter closingSteeringParameter : closingSteeringParameters) {
            if (closingSteeringParameter.getCspType() == 'M') {
                int quarterIndex = (closingSteeringParameter.getCspIndex() - 1) / 3;
                groupedByQuarter
                    .computeIfAbsent(quarterIndex, k -> new ArrayList<>())
                    .add(closingSteeringParameter);
            } else {
                newClosingSteeringParameters.add(closingSteeringParameter);
            }
        }

        for (Map.Entry<Integer, List<ClosingSteeringParameter>> entry : groupedByQuarter.entrySet()) {
            List<ClosingSteeringParameter> monthClosingSteeringParameters = entry.getValue();
            if (monthClosingSteeringParameters.size() != 3) {
                throw new IllegalArgumentException("Cannot convert months to quarters: incomplete quarter data.");
            }
            double totalFactor = monthClosingSteeringParameters.stream().mapToDouble(ClosingSteeringParameter::getFactor).sum();
            newClosingSteeringParameters.add(new ClosingSteeringParameter('Q', 
                                                    entry.getKey() + 1,
                                                    totalFactor,
                                                    monthClosingSteeringParameters.get(0).getDuration()));
        }

        closingSteeringParameters.clear();
        closingSteeringParameters.addAll(newClosingSteeringParameters);
        closingSteeringParameters.sort((u1, u2) -> Integer.compare(u1.getCspIndex(), u2.getCspIndex())); // Sort by cspIndex
        return closingSteeringParameters;
    }

    public static List<PatternElement> toPatternElements(List<ClosingSteeringParameter> closingSteeringParameters) {
        Map<Integer, ClosingSteeringParameter> startPointMap = new HashMap<>();
        closingSteeringParameters.stream()
            .filter(csp -> csp.getCspType() != 'D')
            .forEach(csp -> startPointMap.put(csp.getStartPoint(), csp));

        return closingSteeringParameters.stream()
                .filter(csp -> csp.getCspType() != 'D')
                .map(csp -> {
                    String type;
                    if (csp.getCspType() == 'Y') {
                        type = "YEAR";
                    } else if (csp.getCspType() == 'Q') {
                        type = "QUARTER";
                    } else if (csp.getCspType() == 'M') {
                        type = "MONTH";
                    } else if (csp.getCspType() == 'W') {
                        type = "WEEK";
                    } else {
                        type = "DAY";
                    }

                    double initialDistribution = 0;
                    ClosingSteeringParameter matchingD = closingSteeringParameters.stream()
                            .filter(d -> d.getCspType() == 'D' && d.getStartPoint() == csp.getStartPoint())
                            .findFirst()
                            .orElse(null);

                    if (matchingD != null) {
                        initialDistribution = matchingD.getFactor();
                    }

                    return new PatternElement(
                        initialDistribution,
                        csp.getFactor(),
                        PatternElement.Type.valueOf(type)
                    );
                })
                .toList();
    }

    private char cspType;
    private int cspIndex;
    private double factor;
    private int duration;
    private int startPoint;
    private int endPoint;

    public ClosingSteeringParameter(char cspType, int cspIndex, double factor, int duration) {
        this.cspType = cspType;
        this.cspIndex = cspIndex;
        this.factor = factor;
        this.duration = duration;

        switch (cspType) {
            case 'Y' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.YEAR)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.YEAR));
            }
            case 'Q' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.QUARTER)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.QUARTER));
            }
            case 'M' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.MONTH)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.MONTH));
            }
            case 'W' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.WEEK)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.WEEK));
            }
            case 'D' -> {
            this.startPoint = cspIndex;
            this.endPoint = cspIndex;
            }
            default -> throw new IllegalArgumentException("Invalid cspType: " + cspType);
        }
    }
}
