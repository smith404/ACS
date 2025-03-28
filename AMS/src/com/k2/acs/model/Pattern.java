package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.EnumMap;


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
        double sum = 0;
        for (PatternElement element : elements) {
            sum += element.getDistribution();
        }
        return sum == 1.0;
    }

    public enum Type {
        DAY, MONTH, QUARTER, YEAR
    }
}
