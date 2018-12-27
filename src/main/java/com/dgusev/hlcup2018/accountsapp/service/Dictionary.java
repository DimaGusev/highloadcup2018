package com.dgusev.hlcup2018.accountsapp.service;

import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TObjectByteMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TObjectByteHashMap;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class Dictionary {

    private TByteObjectMap<String> countryDictionary = new TByteObjectHashMap<>();
    private TObjectByteMap<String> countryReverseDictionary = new TObjectByteHashMap<>();
    private AtomicInteger countrySequence = new AtomicInteger();


    public String getCountry(byte country) {
        return countryDictionary.get(country);
    }

    public byte getCountry(String country) {
        return countryReverseDictionary.get(country);
    }

    public byte getOrCreateCountry(String country) {
        if (!countryReverseDictionary.containsKey(country)) {
            int id = countrySequence.incrementAndGet();
            countryDictionary.put((byte)id, country);
            countryReverseDictionary.put(country, (byte)id);
            return (byte) id;
        } else {
            return countryReverseDictionary.get(country);
        }
    }


}
