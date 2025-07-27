package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public class UltimateValue extends PropertyObject {
    private double amount;
    private BaselineElement type;

    public enum BaselineElement {
        PREMIUM, COSTS, LOSSES
    }

    public UltimateValue(BaselineElement type, double amount) {
        this.type = type;
        this.amount = amount;
    }
}
