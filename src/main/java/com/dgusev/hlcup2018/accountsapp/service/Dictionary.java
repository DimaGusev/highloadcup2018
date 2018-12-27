package com.dgusev.hlcup2018.accountsapp.service;

import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectByteMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectByteHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class Dictionary {

    private TByteObjectMap<String> countryDictionary = new TByteObjectHashMap<>();
    private TObjectByteMap<String> countryReverseDictionary = new TObjectByteHashMap<>();
    private AtomicInteger countrySequence = new AtomicInteger();

    private TIntObjectMap<String> cityDictionary = new TIntObjectHashMap<>();
    private TObjectIntMap<String> cityReverseDictionary = new TObjectIntHashMap<>();
    private AtomicInteger citySequence = new AtomicInteger();

    private TIntObjectMap<String> fnameDictionary = new TIntObjectHashMap<>();
    private TObjectIntMap<String> fnameReverseDictionary = new TObjectIntHashMap<>();
    private AtomicInteger fnameSequence = new AtomicInteger();


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

    public String getCity(int city) {
        return cityDictionary.get(city);
    }

    public int getCity(String city) {
        return cityReverseDictionary.get(city);
    }

    public int getOrCreateCity(String city) {
        if (!cityReverseDictionary.containsKey(city)) {
            int id = citySequence.incrementAndGet();
            cityDictionary.put(id, city);
            cityReverseDictionary.put(city, id);
            return id;
        } else {
            return cityReverseDictionary.get(city);
        }
    }

    public String getFname(int fname) {
        return fnameDictionary.get(fname);
    }

    public int getFname(String fname) {
        return fnameReverseDictionary.get(fname);
    }

    public int getOrCreateFname(String city) {
        if (!fnameReverseDictionary.containsKey(city)) {
            int id = fnameSequence.incrementAndGet();
            fnameDictionary.put(id, city);
            fnameReverseDictionary.put(city, id);
            return id;
        } else {
            return fnameReverseDictionary.get(city);
        }
    }


}
