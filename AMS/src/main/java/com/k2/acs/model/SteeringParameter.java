package com.k2.acs.model;

import com.k2.acs.model.PatternElement.Type;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SteeringParameter {
    private char cspType;
    private int cspIndex;
    private double factor;
    private int duration;

    public SteeringParameter(char cspType, int cspIndex, double factor, int duration) {
        this.cspType = cspType;
        this.cspIndex = cspIndex;
        this.factor = factor;
        this.duration = duration;
    }

    public int getStartPoint() {
        switch (cspType) {
            case 'Y' -> {
                return ((cspIndex - 1) * FactorCalculator.getDaysForType(Type.YEAR)) + 1;
            }
            case 'Q' -> {
                return ((cspIndex - 1) * FactorCalculator.getDaysForType(Type.QUARTER)) + 1;
            }
            case 'M' -> {
                return ((cspIndex - 1) * FactorCalculator.getDaysForType(Type.MONTH)) + 1;
            }
            case 'W' -> {
                return ((cspIndex - 1) * FactorCalculator.getDaysForType(Type.WEEK)) + 1;
            }
            case 'D' -> {
                return cspIndex;
            }
            default -> throw new IllegalArgumentException("Invalid cspType: " + cspType);
        }
    }

    public int getEndPoint() {
        switch (cspType) {
            case 'Y' -> {
                return (cspIndex * FactorCalculator.getDaysForType(Type.YEAR));
            }
            case 'Q' -> {
                return (cspIndex * FactorCalculator.getDaysForType(Type.QUARTER));
            }
            case 'M' -> {
                return (cspIndex * FactorCalculator.getDaysForType(Type.MONTH));
            }
            case 'W' -> {
                return (cspIndex * FactorCalculator.getDaysForType(Type.WEEK));
            }
            case 'D' -> {
                return cspIndex;
            }
            default -> throw new IllegalArgumentException("Invalid cspType: " + cspType);
        }
    }
}
