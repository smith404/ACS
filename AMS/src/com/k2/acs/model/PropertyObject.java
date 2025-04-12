package com.k2.acs.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import lombok.ToString;

@ToString
public abstract class PropertyObject {
    protected List<Pair<String, Object>> properties = new ArrayList<>();

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nProperties:\n");
        for (Pair<String, Object> pair : properties) {
            sb.append(String.format("%-20s : %s%n", pair.getKey(), pair.getValue()));
        }
        return sb.toString();
    }
}
