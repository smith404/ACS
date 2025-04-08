package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public class CashFlow extends PropertyObject {
    private double amount;
    private LocalDate date;

    public CashFlow(LocalDate date, double amount) {
        this.date = date;
        this.amount = amount;
    }
}
