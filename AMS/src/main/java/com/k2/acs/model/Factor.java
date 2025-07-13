package com.k2.acs.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;

@Data
@AllArgsConstructor
public class Factor {
    private LocalDate incurredDate;
    private LocalDate exposureDate;
    private double value;
    private boolean isWritten;

    public Factor(LocalDate incurredDate, LocalDate exposureDate, double value) {
        this(incurredDate, exposureDate, value, false);
    }
}
