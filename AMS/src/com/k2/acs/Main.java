package com.k2.acs;

import com.k2.acs.model.UltimateValue;

public class Main {
    public static void main(String[] args) {
        // Create an UltimateValue object
        UltimateValue ultimateValue = new UltimateValue();
        ultimateValue.setAmount(1000.0);
        ultimateValue.setType(UltimateValue.Type.PREMIUM);
        ultimateValue.addProperty("TOA", "1100");
        ultimateValue.addProperty("UV_BASIS", 39);

        // Print the UltimateValue object
        System.out.println(ultimateValue);
    }
}
