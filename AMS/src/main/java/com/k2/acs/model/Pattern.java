package com.k2.acs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pattern {
    private final String uuid = UUID.randomUUID().toString();
    private List<PatternFactor> elements = new ArrayList<>();

    public void addElement(PatternFactor element) {
        if (elements.contains(element)) {
            throw new IllegalArgumentException("Element already exists in the pattern.");
        }
        elements.add(element);
    }

    public void removeElement(PatternElement element) {
        if (!elements.contains(element)) {
            throw new IllegalArgumentException("Element does not exist in the pattern.");
        }
        elements.remove(element);
    }

    public boolean isDistributionValid() {
        double sumDistribution = 0;
        for (PatternFactor element : elements) {
            sumDistribution += element.getDistribution();
            sumDistribution += element.getUpFront();
        }
        return sumDistribution == 1.0;
    }
}
