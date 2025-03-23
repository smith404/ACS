package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pattern {
    private String type;
    private List<PatternElement> elements = new ArrayList<>();
    private int contractDuration;

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
