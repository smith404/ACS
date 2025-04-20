package com.k2.acs.model;

import lombok.Data;

import java.time.LocalDate;

@Data
public class Factor {
    private LocalDate originDate;
    private double distribution;
    private LocalDate date;
    private int developmentPeriod;
    private double value;

    public Factor(LocalDate originDate, double distribution, LocalDate date, double value) {
        this.originDate = originDate;
        this.distribution = distribution;
        this.date = date;
        this.value = value;
    }

    public Factor(LocalDate originDate, double distribution, LocalDate date) {
        this(originDate, distribution, date, 0);
    }
}
