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
}
