package com.k2.acs.model;

import java.util.List;

import lombok.Data;

@Data
public class Scenario {
    private String group;
    private String key;
    private String name;
    private String description;
    private boolean releasable;
    private List<Factor> factors;

    @Data
    public static class Factor {
        private String type;
        private double distribution;
    }
}
