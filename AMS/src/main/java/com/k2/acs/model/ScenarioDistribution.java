package com.k2.acs.model;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ScenarioDistribution extends Scenario {
    private List<Factor> factors;

    @Data
    public static class Factor {
        private String type;
        private double distribution;
    }
    
}
