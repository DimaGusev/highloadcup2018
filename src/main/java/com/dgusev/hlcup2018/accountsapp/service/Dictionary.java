package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import gnu.trove.impl.Constants;
import gnu.trove.map.*;
import gnu.trove.map.hash.*;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class Dictionary {

    private TByteObjectMap<String> countryDictionary = new TByteObjectHashMap<>();
    private TByteObjectMap<byte[]> countryDictionaryBytes = new TByteObjectHashMap<>();
    private TObjectByteMap<String> countryReverseDictionary = new TObjectByteHashMap<>();
    private AtomicInteger countrySequence = new AtomicInteger();

    private TIntObjectMap<String> cityDictionary = new TIntObjectHashMap<>();
    private TIntObjectMap<byte[]> cityDictionaryBytes = new TIntObjectHashMap<>();
    private TObjectIntMap<String> cityReverseDictionary = new TObjectIntHashMap<>();
    private AtomicInteger citySequence = new AtomicInteger();

    private TIntObjectMap<String> fnameDictionary = new TIntObjectHashMap<>();
    private TIntObjectMap<byte[]> fnameDictionaryBytes = new TIntObjectHashMap<>();
    private TObjectIntMap<String> fnameReverseDictionary = new TObjectIntHashMap<>();
    private AtomicInteger fnameSequence = new AtomicInteger();
    private TIntByteMap fnameSexDictionary = new TIntByteHashMap();


    private TIntObjectMap<String> snameDictionary = new TIntObjectHashMap<>();
    private TIntObjectMap<byte[]> snameDictionaryBytes = new TIntObjectHashMap<>();
    private TObjectIntMap<String> snameReverseDictionary = new TObjectIntHashMap<>();
    private AtomicInteger snameSequence = new AtomicInteger();

    private TByteObjectMap<String> interesDictionary = new TByteObjectHashMap<>();
    private TByteObjectMap<byte[]> interesDictionaryBytes = new TByteObjectHashMap<>();
    private TObjectByteMap<String> interesReverseDictionary = new TObjectByteHashMap<>();
    private AtomicInteger interesSequence = new AtomicInteger();



    public String getCountry(byte country) {
        return countryDictionary.get(country);
    }

    public byte[] getCountryBytes(byte country) {
        return countryDictionaryBytes.get(country);
    }

    public byte getCountry(String country) {
        return countryReverseDictionary.get(country);
    }

    public byte getOrCreateCountry(String country) {
        if (!countryReverseDictionary.containsKey(country)) {
            int id = countrySequence.incrementAndGet();
            countryDictionary.put((byte)id, country);
            countryDictionaryBytes.put((byte)id, country.getBytes());
            countryReverseDictionary.put(country, (byte)id);
            return (byte) id;
        } else {
            return countryReverseDictionary.get(country);
        }
    }

    public String getCity(int city) {
        return cityDictionary.get(city);
    }

    public byte[] getCityBytes(int city) {
        return cityDictionaryBytes.get(city);
    }

    public int getCity(String city) {
        return cityReverseDictionary.get(city);
    }

    public int getOrCreateCity(String city) {
        if (!cityReverseDictionary.containsKey(city)) {
            int id = citySequence.incrementAndGet();
            cityDictionary.put(id, city);
            cityDictionaryBytes.put(id, city.getBytes());
            cityReverseDictionary.put(city, id);
            return id;
        } else {
            return cityReverseDictionary.get(city);
        }
    }

    public String getFname(int fname) {
        return fnameDictionary.get(fname);
    }

    public byte[] getFnameBytes(int fname) {
        return fnameDictionaryBytes.get(fname);
    }

    public int getFname(String fname) {
        return fnameReverseDictionary.get(fname);
    }

    public int getOrCreateFname(String fname) {
        if (!fnameReverseDictionary.containsKey(fname)) {
            int id = fnameSequence.incrementAndGet();
            fnameDictionary.put(id, fname);
            fnameDictionaryBytes.put(id, fname.getBytes());
            fnameReverseDictionary.put(fname, id);
            return id;
        } else {
            return fnameReverseDictionary.get(fname);
        }
    }

    public void updateFnameSexDictionary(int fname, boolean sex) {
        byte value = fnameSexDictionary.get(fname);
        if (value == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
            fnameSexDictionary.put(fname, sex ? (byte)2: 1);
        } else {
            byte current = sex ? (byte)2: 1;
            if (value != current) {
                fnameSexDictionary.put(fname, (byte)3);
            }
        }
    }

    public byte getFnameSex(int fname) {
        return fnameSexDictionary.get(fname);
    }


    public String getSname(int sname) {
        return snameDictionary.get(sname);
    }

    public byte[] getSnameBytes(int sname) {
        return snameDictionaryBytes.get(sname);
    }

    public int getSname(String sname) {
        return snameReverseDictionary.get(sname);
    }

    public int getOrCreateSname(String sname) {
        if (!snameReverseDictionary.containsKey(sname)) {
            int id = snameSequence.incrementAndGet();
            snameDictionary.put(id, sname);
            snameDictionaryBytes.put(id, sname.getBytes());
            snameReverseDictionary.put(sname, id);
            return id;
        } else {
            return snameReverseDictionary.get(sname);
        }
    }


    public String getInteres(byte interes) {
        return interesDictionary.get(interes);
    }

    public byte[] getInteresBytes(byte interes) {
        return interesDictionaryBytes.get(interes);
    }

    public byte getInteres(String interes) {
        return interesReverseDictionary.get(interes);
    }

    public byte getOrCreateInteres(String interes) {
        if (!interesReverseDictionary.containsKey(interes)) {
            int id = interesSequence.incrementAndGet();
            interesDictionary.put((byte)id, interes);
            interesDictionaryBytes.put((byte)id, interes.getBytes());
            interesReverseDictionary.put(interes, (byte)id);
            return (byte) id;
        } else {
            return interesReverseDictionary.get(interes);
        }
    }


}
