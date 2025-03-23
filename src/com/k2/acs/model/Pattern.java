package com.k2.acs.model;

import java.util.ArrayList;
import java.util.List;

public class Pattern {
    private String type;
    private List<PatternElement> elements;

    public Pattern(String type) {
        this.type = type;
        this.elements = new ArrayList<>();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<PatternElement> getElements() {
        return elements;
    }

    public void addElement(PatternElement element) {
        elements.add(element);
    }

    public void addElement(int index, PatternElement element) {
        elements.add(index, element);
    }

    public void removeElement(PatternElement element) {
        elements.remove(element);
    }
}
