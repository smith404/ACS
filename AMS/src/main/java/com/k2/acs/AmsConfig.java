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

    private Date valuationDate;

    private int precision = 6;

    private boolean calendar = true;
    private boolean endOfPeriod = false;

    private Date insuredPeriodStartDate;
    private int riskAttachingDuration = 0;

    private String inccuredTimeUnit;
    private String exposedTimeUnit;

    private List<UV> ultimateValues;

    private String factorType;

    private List<Element> elements;

    public LocalDate getValuationDateAsLocalDate() {
        return valuationDate != null ? valuationDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getInsuredPeriodStartDateAsLocalDate() {
        return insuredPeriodStartDate != null ? insuredPeriodStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    @Data
    public static class Element {
        private String type;
        private Double initial = 0.0;
        private Double distribution = 0.0;
        private int riskAttachingDuration = -1;
    }

    @Data
    public static class UV {
        private String type = "PREMIUM";
        private Double value = 1.0;
    }

    public static class ConfigParseException extends Exception {
        public ConfigParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
