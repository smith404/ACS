package com.k2.acs.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UltimateValue {
    private double amount;
    private Type type;
    private List<Pair<String, Object>> properties = new ArrayList<>();

    public enum Type {
        PREMIUM, COST, CLAIM
    }

    public void addProperty(String key, Object value) {
        properties.add(new MutablePair<>(key, value));
    }

    public void removeProperty(String key) {
        properties.removeIf(pair -> pair.getKey().equals(key));
    }

    public Object getProperty(String key) {
        for (Pair<String, Object> pair : properties) {
            if (pair.getKey().equals(key)) {
                return pair.getValue();
            }
        }
        return null;
    }
}
