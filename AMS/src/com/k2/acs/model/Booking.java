package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public class Booking extends PropertyObject {
    private double amount;
    private String currency;

    public Booking(double amount, String currency) {
        this.amount = amount;
        this.currency = currency;
    }
}
