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
    private String currency;
    private LocalDate incurredDate;
    private LocalDate reportedDate;
    private LocalDate dueDate;
    private LocalDate settlementDate;

    public CashFlow(LocalDate incurredDate, double amount) {
        this.incurredDate = incurredDate;
        this.amount = amount;
        inferOtherDates();
    }

    public void inferOtherDates() {
        if (this.reportedDate == null) {
            this.reportedDate = this.incurredDate.plusMonths(3);
        }
        if (this.dueDate == null) {
            this.dueDate = this.incurredDate.plusMonths(6);
        }
        if (this.settlementDate == null) {
            this.settlementDate = this.incurredDate.plusYears(1);
        }
    }
}
