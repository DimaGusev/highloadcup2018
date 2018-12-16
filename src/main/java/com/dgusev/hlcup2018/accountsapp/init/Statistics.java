package com.dgusev.hlcup2018.accountsapp.init;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Statistics {

    int count = 0;
    int minId = Integer.MAX_VALUE;
    int maxId = Integer.MIN_VALUE;
    Set<String> emailsDomains = new HashSet<>();
    int fnameNotNull = 0;
    Set<String> fnames = new HashSet<>();
    int snameNotNull = 0;
    Set<String> snames = new HashSet<>();
    int phoneNotNull = 0;
    Set<String> phoneCodes = new HashSet<>();
    int mcount = 0;
    Set<String> country = new HashSet<>();
    int countryNotNull = 0;
    Set<String> city = new HashSet<>();
    int cityNotNull = 0;
    int[] statuses =new int[3];
    Set<String> interests = new HashSet<>();
    int premiumCount = 0;
    int likesCount = 0;


    public void analyze(AccountDTO accountDTO) {
        count++;
        minId = Math.min(minId, accountDTO.id);
        maxId = Math.max(maxId, accountDTO.id);
        int index = accountDTO.email.lastIndexOf('@');
        emailsDomains.add(accountDTO.email.substring(index + 1));
        if (accountDTO.fname != null) {
            fnameNotNull++;
            fnames.add(accountDTO.fname);
        }
        if (accountDTO.sname != null) {
            snameNotNull++;
            snames.add(accountDTO.sname);
        }
        if (accountDTO.phone != null) {
            phoneNotNull++;
            int from = accountDTO.phone.indexOf('(');
            int to = accountDTO.phone.indexOf(')');
            if (from != -1 && to != -1) {
                phoneCodes.add(accountDTO.phone.substring(from + 1, to));
            }
        }
        if (accountDTO.sex.equals("m")) {
            mcount++;
        }
        if (accountDTO.country != null) {
            countryNotNull++;
            country.add(accountDTO.country);
        }

        if (accountDTO.city != null) {
            cityNotNull++;
            city.add(accountDTO.city);
        }

        if (accountDTO.status.equals("свободны")) {
            statuses[0]++;
        } else if (accountDTO.status.equals("всё сложно")) {
            statuses[1]++;
        } else {
            statuses[2]++;
        }

        if (accountDTO.interests != null) {
            interests.addAll(accountDTO.interests);
        }

        if (accountDTO.premiumStart != 0) {
            premiumCount++;
        }

        if (accountDTO.likes != null) {
            likesCount+= accountDTO.likes.size();
        }
    }

    @Override
    public String toString() {
        return "Statistics{" +
                "count=" + count +
                ", minId=" + minId +
                ", maxId=" + maxId +
                ", emailsDomains=" + emailsDomains.size() +
                ", fnameNotNull=" + fnameNotNull +
                ", fnames=" + fnames.size() +
                ", snameNotNull=" + snameNotNull +
                ", snames=" + snames.size() +
                ", phoneNotNull=" + phoneNotNull +
                ", phoneCodes=" + phoneCodes.size() +
                ", mcount=" + mcount +
                ", country=" + country.size() +
                ", countryNotNull=" + countryNotNull +
                ", city=" + city.size() +
                ", cityNotNull=" + cityNotNull +
                ", statuses=" + Arrays.toString(statuses) +
                ", interests=" + interests.size() +
                ", premiumCount=" + premiumCount +
                ", likesCount=" + likesCount +
                '}';
    }
}
