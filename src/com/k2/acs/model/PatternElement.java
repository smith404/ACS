package com.k2.acs.model;

import lombok.Data;
import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
public class PatternElement {
    private double distribution;
    private Type type;

    public enum Type {
        DAY, MONTH, QUARTER, YEAR
    }
}
