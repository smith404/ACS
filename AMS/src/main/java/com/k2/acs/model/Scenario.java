package com.k2.acs.model;

import lombok.Data;

@Data
public class Scenario {
    private String group;
    private String key;
    private String name;
    private String description;
    private boolean releasable;
}
