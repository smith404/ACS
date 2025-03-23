package com.k2.acs.model;

import lombok.Data;
import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
public class UltimateValue {
    private double amount;
    private Type type;
    private String typeOfAmount;

    public enum Type {
        PREMIUM, COST, CLAIM
    }
}
