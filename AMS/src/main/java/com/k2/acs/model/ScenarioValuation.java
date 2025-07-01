package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ScenarioValuation extends Scenario{
    private String financialPeriod;
    private String currency;
    private double amount;
}
