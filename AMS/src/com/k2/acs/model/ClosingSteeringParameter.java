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

import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClosingSteeringParameter {

    public static List<ClosingSteeringParameter> parseUnitsFromStream(InputStream inputStream, boolean hasHeaderLine, String delimiter) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            return reader.lines()
                         .skip(hasHeaderLine ? 1 : 0)
                         .map(line -> {
                             String[] parts = line.split(delimiter);
                             if (parts.length != 3) {
                                 throw new IllegalArgumentException("Invalid line format: " + line);
                             }
                             char unitType = parts[0].charAt(0);
                             int unitTypeCount = Integer.parseInt(parts[1]);
                             double factor = Double.parseDouble(parts[2]);
                             return new ClosingSteeringParameter(unitType, unitTypeCount, factor);
                         })
                         .toList();
        }
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
            if (closingSteeringParameter.getCspType() == 'D' && (closingSteeringParameter.getCspTypeCount() - 1) % 90 != 0) {
                throw new IllegalArgumentException("Invalid cspTypeCount for 'D' when 'Q' is present.");
            }
        }
    }

    private static void validateMClosingSteeringParameters(List<ClosingSteeringParameter> closingSteeringParameters) {
        for (ClosingSteeringParameter closingSteeringParameter : closingSteeringParameters) {
            if (closingSteeringParameter.getCspType() == 'D' && (closingSteeringParameter.getCspTypeCount() - 1) % 30 != 0) {
                throw new IllegalArgumentException("Invalid cspTypeCount for 'D' when 'M' is present.");
            }
        }
    }

    public static List<ClosingSteeringParameter> convertQuartersToMonths(List<ClosingSteeringParameter> closingSteeringParameters) {
        List<ClosingSteeringParameter> newClosingSteeringParameters = new ArrayList<>();

        for (ClosingSteeringParameter closingSteeringParameter : closingSteeringParameters) {
            if (closingSteeringParameter.getCspType() == 'Q') {
                int mCounter = 1;
                for (int i = 0; i < 3; i++) {
                    newClosingSteeringParameters.add(new ClosingSteeringParameter('M', (((closingSteeringParameter.cspTypeCount-1) * 3) + mCounter++), closingSteeringParameter.getFactor()/3));
                }
            } else {
                newClosingSteeringParameters.add(closingSteeringParameter);
            }
        }

        closingSteeringParameters.clear();
        closingSteeringParameters.addAll(newClosingSteeringParameters);
        closingSteeringParameters.sort((u1, u2) -> Integer.compare(u1.getCspTypeCount(), u2.getCspTypeCount())); // Sort by cspTypeCount
        return closingSteeringParameters;
    }

    public static List<ClosingSteeringParameter> convertMonthsToQuarters(List<ClosingSteeringParameter> closingSteeringParameters) {
        List<ClosingSteeringParameter> newClosingSteeringParameters = new ArrayList<>();
        Map<Integer, List<ClosingSteeringParameter>> groupedByQuarter = new HashMap<>();

        for (ClosingSteeringParameter closingSteeringParameter : closingSteeringParameters) {
            if (closingSteeringParameter.getCspType() == 'M') {
                int quarterIndex = (closingSteeringParameter.getCspTypeCount() - 1) / 3;
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
            newClosingSteeringParameters.add(new ClosingSteeringParameter('Q', entry.getKey() + 1, totalFactor));
        }

        closingSteeringParameters.clear();
        closingSteeringParameters.addAll(newClosingSteeringParameters);
        closingSteeringParameters.sort((u1, u2) -> Integer.compare(u1.getCspTypeCount(), u2.getCspTypeCount())); // Sort by cspTypeCount
        return closingSteeringParameters;
    }

    private char cspType;
    private int cspTypeCount;
    private double factor;
}
