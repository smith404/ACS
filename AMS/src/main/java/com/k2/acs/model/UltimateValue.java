package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public class UltimateValue extends PropertyObject {
    private double amount;
    private Type type;

    public enum Type {
        PREMIUM, COSTS, LOSSES
    }

    public UltimateValue(Type type, double amount) {
        this.type = type;
        this.amount = amount;
    }

    public UltimateValue(BestEstimateCashFlow becf) {
        throw new UnsupportedOperationException("Constructor not implemented");
    }
}
