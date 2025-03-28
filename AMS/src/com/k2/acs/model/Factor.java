package com.k2.acs.model;

import lombok.Data;
import java.time.LocalDate;

@Data
public class Factor {
    private double distribution;
    private LocalDate date;

    public Factor(double distribution, LocalDate date) {
        this.distribution = distribution;
        this.date = date;
    }
}
