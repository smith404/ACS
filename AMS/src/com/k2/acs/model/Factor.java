package com.k2.acs.model;

import lombok.Data;
import java.time.LocalDate;

@Data
public class Factor {
    private double distribution;
    private LocalDate date;
    private int developmentPeriod;
    private double value;

    public Factor(double distribution, LocalDate date, double value) {
        this.distribution = distribution;
        this.date = date;
        this.value = value;
    }

    public Factor(double distribution, LocalDate date) {
        this(distribution, date, 0);
    }
}
