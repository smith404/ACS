package com.k2.acs;

import java.util.Date;
import java.util.List;

import lombok.Data;

import java.time.LocalDate;
import java.time.ZoneId;

@Data
public class AmsConfig {
    private int precision = 6;
    private Date lbd;
    private boolean showPastFuture;
    private boolean endOfPeriod = false;
    private String cashFlowFrequency;
    private Date cashFlowStart;
    private Date cashFlowEnd;
    private double amount;
    private String currency;
    private String toa;
    private int duration;
    private boolean calendar;
    private Date contractDate;
    private String patternType;
    private String factor;
    private List<Element> elements;

    public LocalDate getLbdAsLocalDate() {
        return lbd != null ? lbd.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getCashFlowStartAsLocalDate() {
        return cashFlowStart != null ? cashFlowStart.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getCashFlowEndAsLocalDate() {
        return cashFlowEnd != null ? cashFlowEnd.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getContractDateAsLocalDate() {
        return contractDate != null ? contractDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    @Data
    public static class Element {
        private String type;
        private Double initial = 0.0;
        private Double distribution = 0.0;
    }
}
