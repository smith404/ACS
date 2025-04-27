package com.k2.acs;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

import java.io.File;
import java.time.LocalDate;
import java.time.ZoneId;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AmsConfig {
    public static AmsConfig parseConfig(String configFilePath) throws ConfigParseException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(new File(configFilePath), AmsConfig.class);
        } catch (Exception e) {
            throw new ConfigParseException("Failed to parse configuration file: " + configFilePath, e);
        }
    }

    private int precision = 6;

    private boolean calendar;
    private Date lbd;
    private boolean endOfPeriod = false;
    private String cashFlowFrequency;

    private String toa;
    private String currency;
    private double amount;

    private Date insuredPeriodStartDate;
    private String patternType;
    private String factorType;
    private int duration;

    private List<Element> elements;

    public LocalDate getLbdAsLocalDate() {
        return lbd != null ? lbd.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getInsuredPeriodStartDateAsLocalDate() {
        return insuredPeriodStartDate != null ? insuredPeriodStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    @Data
    public static class Element {
        private String type;
        private Double initial = 0.0;
        private Double distribution = 0.0;
    }

    public static class ConfigParseException extends Exception {
        public ConfigParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
