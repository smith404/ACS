package com.k2.acs.model;

import lombok.Data;
import java.util.Date;

@Data
public class Factor {
    private double distribution;
    private Date date;

    public Factor(double distribution, Date date) {
        this.distribution = distribution;
        this.date = date;
    }
}
