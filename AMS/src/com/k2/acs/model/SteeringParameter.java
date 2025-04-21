package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import com.k2.acs.model.PatternElement.Type;

@Data
@NoArgsConstructor
public class SteeringParameter {
    private char cspType;
    private int cspIndex;
    private double factor;
    private int duration;
    private int startPoint;
    private int endPoint;

    public SteeringParameter(char cspType, int cspIndex, double factor, int duration) {
        this.cspType = cspType;
        this.cspIndex = cspIndex;
        this.factor = factor;
        this.duration = duration;
    }

    public int getStartPoint() {
        switch (cspType) {
            case 'Y' -> {
                return ((cspIndex - 1) * Calculator.getDaysForType(Type.YEAR)) + 1;
            }
            case 'Q' -> {
                return ((cspIndex - 1) * Calculator.getDaysForType(Type.QUARTER)) + 1;
            }
            case 'M' -> {
                return ((cspIndex - 1) * Calculator.getDaysForType(Type.MONTH)) + 1;
            }
            case 'W' -> {
                return ((cspIndex - 1) * Calculator.getDaysForType(Type.WEEK)) + 1;
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
                return (cspIndex * Calculator.getDaysForType(Type.YEAR));
            }
            case 'Q' -> {
                return (cspIndex * Calculator.getDaysForType(Type.QUARTER));
            }
            case 'M' -> {
                return(cspIndex * Calculator.getDaysForType(Type.MONTH));
            }
            case 'W' -> {
                return (cspIndex * Calculator.getDaysForType(Type.WEEK));
            }
            case 'D' -> {
                return cspIndex;
            }
            default -> throw new IllegalArgumentException("Invalid cspType: " + cspType);
        }
    }
}
