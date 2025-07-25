package com.k2.acs.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;

@Data
@AllArgsConstructor
public class Factor {
    public enum Type {
        UPFRONT, DIST, START, END
    }
    private LocalDate incurredDate;
    private LocalDate exposureDate;
    private double value;
    private Type factorType;
}
