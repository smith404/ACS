package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pattern {
    private String type;
    private List<PatternElement> elements = new ArrayList<>();
    private int duration = 0;

    public void addElement(PatternElement element, boolean transfer) {
        if (element.getParentPattern() != null && element.getParentPattern() != this) {
            if (transfer) {
                element.setParentPattern(this);
            } else {
                throw new IllegalArgumentException("Element already belongs to another pattern.");
            }
        }
        if (elements.contains(element)) {
            throw new IllegalArgumentException("Element already exists in the pattern.");
        }
        element.setParentPattern(this);
        elements.add(element);
    }

    public void addElement(int index, PatternElement element, boolean transfer) {
        if (element.getParentPattern() != null && element.getParentPattern() != this) {
            if (transfer) {
                element.setParentPattern(this);
            } else {
                throw new IllegalArgumentException("Element already belongs to another pattern.");
            }
        }
        if (elements.contains(element)) {
            throw new IllegalArgumentException("Element already exists in the pattern.");
        }
        element.setParentPattern(this);
        elements.add(index, element);
    }

    public void addElement(PatternElement element) {
        addElement(element, false);
    }

    public void addElement(int index, PatternElement element) {
        addElement(index, element, false);
    }

    public void removeElement(PatternElement element) {
        if (!elements.contains(element)) {
            throw new IllegalArgumentException("Element does not exist in the pattern.");
        }
        if (element.getParentPattern() != null && element.getParentPattern() == this) {
            element.setParentPattern(null);
        }
   
        elements.remove(element);
    }

    public boolean isDistributionValid() {
        double sumDistribution = 0;
        double sumInitialDistribution = 0;
        for (PatternElement element : elements) {
            sumDistribution += element.getDistribution();
            sumInitialDistribution += element.getInitialDistribution();
        }
        return sumDistribution == 1.0 && sumInitialDistribution == 1.0;
    }

    public List<Unit> parseUnitsFromStream(InputStream inputStream, boolean hasHeaderLine, String delimiter) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            return reader.lines()
                         .skip(hasHeaderLine ? 1 : 0) // Conditionally skip header line
                         .map(line -> {
                             String[] parts = line.split(delimiter);
                             if (parts.length != 3) {
                                 throw new IllegalArgumentException("Invalid line format: " + line);
                             }
                             char unitType = parts[0].charAt(0);
                             int unitTypeCount = Integer.parseInt(parts[1]);
                             double factor = Double.parseDouble(parts[2]);
                             return new Unit(unitType, unitTypeCount, factor);
                         })
                         .toList();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Unit {
        public static void validate(List<Unit> units) {
            boolean hasQ = units.stream().anyMatch(unit -> unit.getUnitType() == 'Q');
            boolean hasM = units.stream().anyMatch(unit -> unit.getUnitType() == 'M');
            boolean hasD = units.stream().anyMatch(unit -> unit.getUnitType() == 'D');

            if (hasQ && (hasM || hasD)) {
                validateQUnits(units, hasM);
            }

            if (hasM) {
                validateMUnits(units);
            }
        }

        private static void validateQUnits(List<Unit> units, boolean hasM) {
            if (hasM) {
                throw new IllegalArgumentException("Invalid cannot mix 'M' and 'Q' units.");
            }
            for (Unit unit : units) {
                if (unit.getUnitType() == 'D' && (unit.getUnitTypeCount() - 1) % 90 != 0) {
                    throw new IllegalArgumentException("Invalid unitTypeCount for 'D' when 'Q' is present.");
                }
            }
        }

        private static void validateMUnits(List<Unit> units) {
            for (Unit unit : units) {
                if (unit.getUnitType() == 'D' && (unit.getUnitTypeCount() - 1) % 30 != 0) {
                    throw new IllegalArgumentException("Invalid unitTypeCount for 'D' when 'M' is present.");
                }
            }
        }

        public static List<Unit> convertQuartersToMonths(List<Unit> units) {
            List<Unit> newUnits = new ArrayList<>();

            for (Unit unit : units) {
                if (unit.getUnitType() == 'Q') {
                    int mCounter = 1;
                    for (int i = 0; i < 3; i++) {
                        newUnits.add(new Unit('M', (((unit.unitTypeCount-1) * 3) + mCounter++), unit.getFactor()/3));
                    }
                } else {
                    newUnits.add(unit);
                }
            }

            units.clear();
            units.addAll(newUnits);
            units.sort((u1, u2) -> Integer.compare(u1.getUnitTypeCount(), u2.getUnitTypeCount())); // Sort by unitTypeCount
            return units;
        }

        public static List<Unit> convertMonthsToQuarters(List<Unit> units) {
            List<Unit> newUnits = new ArrayList<>();
            Map<Integer, List<Unit>> groupedByQuarter = new HashMap<>();

            for (Unit unit : units) {
                if (unit.getUnitType() == 'M') {
                    int quarterIndex = (unit.getUnitTypeCount() - 1) / 3;
                    groupedByQuarter
                        .computeIfAbsent(quarterIndex, k -> new ArrayList<>())
                        .add(unit);
                } else {
                    newUnits.add(unit);
                }
            }

            for (Map.Entry<Integer, List<Unit>> entry : groupedByQuarter.entrySet()) {
                List<Unit> monthUnits = entry.getValue();
                if (monthUnits.size() != 3) {
                    throw new IllegalArgumentException("Cannot convert months to quarters: incomplete quarter data.");
                }
                double totalFactor = monthUnits.stream().mapToDouble(Unit::getFactor).sum();
                newUnits.add(new Unit('Q', entry.getKey() + 1, totalFactor));
            }

            units.clear();
            units.addAll(newUnits);
            units.sort((u1, u2) -> Integer.compare(u1.getUnitTypeCount(), u2.getUnitTypeCount())); // Sort by unitTypeCount
            return units;
        }

        private char unitType;
        private int unitTypeCount;
        private double factor;
    }
}
