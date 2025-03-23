package com.k2.acs;

import com.k2.acs.model.UltimateValue;
import com.k2.acs.model.UltimateValue.Type;

public class Main {
    public static void main(String[] args) {
        UltimateValue ultimateValue = new UltimateValue(1000.0, Type.PREMIUM, "Annual Premium");
        System.out.println("Amount: " + ultimateValue.getAmount());
        System.out.println("Type: " + ultimateValue.getType());
        System.out.println("Type of Amount: " + ultimateValue.getTypeOfAmount());
    }
}
