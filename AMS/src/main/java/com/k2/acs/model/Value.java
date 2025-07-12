package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.AllArgsConstructor;

import java.time.LocalDate;

@Data
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class Value extends PropertyObject {
    private LocalDate incurredDate;
    private LocalDate exposureDate;
    private double amount;
    private int toa;
}
