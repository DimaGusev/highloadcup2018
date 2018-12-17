package com.dgusev.hlcup2018.accountsapp.service;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class Dictionary {

    private AtomicInteger fnameCounter = new AtomicInteger(1);
    private AtomicInteger snameCounter = new AtomicInteger(1);
    private AtomicInteger countryCounter = new AtomicInteger(1);
    private AtomicInteger cityCounter = new AtomicInteger(1);

    private Map<String, Integer> fnameMap1 = new HashMap<>();
    private Map<String, Integer> snameMap1 = new HashMap<>();
    private Map<String, Integer> countryMap1 = new HashMap<>();
    private Map<String, Integer> cityMap1 = new HashMap<>();

    private Map<Integer, String> fnameMap2 = new HashMap<>();
    private Map<Integer, String> snameMap2 = new HashMap<>();
    private Map<Integer, String> countryMap2 = new HashMap<>();
    private Map<Integer, String> cityMap2 = new HashMap<>();

    public int getFnameNumber(String fName) {
        Integer value = fnameMap1.get(fName);
        if (value != null) {
            return value;
        } else {
            int newValue = fnameCounter.incrementAndGet();
            fnameMap1.put(fName, newValue);
            fnameMap2.put(newValue, fName);
            return newValue;
        }
    }

    public String getFnameValue(int fName) {
        return  fnameMap2.get(fName);
    }

    public int getSnameNumber(String sName) {
        Integer value = snameMap1.get(sName);
        if (value != null) {
            return value;
        } else {
            int newValue = snameCounter.incrementAndGet();
            snameMap1.put(sName, newValue);
            snameMap2.put(newValue, sName);
            return newValue;
        }
    }

    public String getSnameValue(int sName) {
        return  snameMap2.get(sName);
    }

    public int getCountryNumber(String country) {
        Integer value = countryMap1.get(country);
        if (value != null) {
            return value;
        } else {
            int newValue = countryCounter.incrementAndGet();
            countryMap1.put(country, newValue);
            countryMap2.put(newValue, country);
            return newValue;
        }
    }

    public String getCountryValue(int country) {
        return  countryMap2.get(country);
    }

    public int getCityNumber(String city) {
        Integer value = cityMap1.get(city);
        if (value != null) {
            return value;
        } else {
            int newValue = cityCounter.incrementAndGet();
            cityMap1.put(city, newValue);
            cityMap2.put(newValue, city);
            return newValue;
        }
    }

    public String getCityValue(int city) {
        return cityMap2.get(city);
    }




}
