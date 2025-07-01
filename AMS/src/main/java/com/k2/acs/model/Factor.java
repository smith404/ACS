package com.k2.acs.model;

import lombok.Data;

import java.time.LocalDate;

@Data
public class Factor {
    private LocalDate incurredDate;
    private double distribution;
    private LocalDate exposureDate;
    private double value;

    public Factor(LocalDate incurredDate, double distribution, LocalDate exposureDate, double value) {
        this.incurredDate = incurredDate;
        this.distribution = distribution;
        this.exposureDate = exposureDate;
        this.value = value;
    }

    public Factor(LocalDate incurredDate, double distribution, LocalDate exposureDate) {
        this(incurredDate, distribution, exposureDate, 0);
    }
}
