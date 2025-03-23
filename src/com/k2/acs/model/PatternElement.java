package com.k2.acs.model;

public class PatternElement {
    private double distribution;
    private Type type;

    public PatternElement(double distribution, Type type) {
        this.distribution = distribution;
        this.type = type;
    }

    public double getDistribution() {
        return distribution;
    }

    public void setDistribution(double distribution) {
        this.distribution = distribution;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public enum Type {
        DAY, MONTH, QUARTER, YEAR
    }
}
