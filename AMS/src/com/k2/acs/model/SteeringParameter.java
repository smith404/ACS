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

        switch (cspType) {
            case 'Y' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.YEAR)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.YEAR));
            }
            case 'Q' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.QUARTER)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.QUARTER));
            }
            case 'M' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.MONTH)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.MONTH));
            }
            case 'W' -> {
            this.startPoint = ((cspIndex - 1) * Calculator.getDaysForType(Type.WEEK)) + 1;
            this.endPoint = (cspIndex * Calculator.getDaysForType(Type.WEEK));
            }
            case 'D' -> {
            this.startPoint = cspIndex;
            this.endPoint = cspIndex;
            }
            default -> throw new IllegalArgumentException("Invalid cspType: " + cspType);
        }
    }
}
