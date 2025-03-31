package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.EnumMap;
import java.util.Date;
import java.util.Calendar;
import java.time.LocalDate;
import java.util.stream.Collectors;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pattern {
    private String type;
    private List<PatternElement> elements = new ArrayList<>();
    private int contractDuration;

    private static final Map<Type, Integer> typeToDaysMap = new EnumMap<>(Type.class);

    static {
        typeToDaysMap.put(Type.DAY, 1);
        typeToDaysMap.put(Type.WEEK, 7);
        typeToDaysMap.put(Type.MONTH, 30);
        typeToDaysMap.put(Type.QUARTER, 90);
        typeToDaysMap.put(Type.YEAR, 360);
    }

    public static void updateTypeToDays(Type type, int days) {
        typeToDaysMap.put(type, days);
    }

    public static int getDaysForType(Type type) {
        return typeToDaysMap.getOrDefault(type, 0);
    }

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

    public List<Factor> iterateElementsWithStartDate(LocalDate startDate) {
        List<Factor> factors = new ArrayList<>();
        for (PatternElement element : elements) {
            factors.addAll(element.generateFactors(startDate));
            startDate = startDate.plusDays(element.getLength());
        }
        return factors;
    }

    public Pattern add(Pattern other) {
        if (!this.type.equals(other.type)) {
            throw new IllegalArgumentException("Patterns must have the same type to be added.");
        }

        Pattern result = new Pattern();
        result.setType(this.type);
        result.setContractDuration(this.contractDuration + other.contractDuration);

        List<PatternElement> combinedElements = new ArrayList<>(this.elements);
        combinedElements.addAll(other.elements);
        result.setElements(combinedElements);

        return result;
    }

    public enum Type {
        DAY, WEEK, MONTH, QUARTER, YEAR
    }
}
