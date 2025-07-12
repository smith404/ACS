package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

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
