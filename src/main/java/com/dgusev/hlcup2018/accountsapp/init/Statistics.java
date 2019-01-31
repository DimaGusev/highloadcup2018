package com.dgusev.hlcup2018.accountsapp.init;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Statistics {

    int count = 0;
    int minId = Integer.MAX_VALUE;
    int maxId = Integer.MIN_VALUE;
    Set<String> emailsDomains = new HashSet<>();
    int fnameNotNull = 0;
    Set<Integer> fnames = new HashSet<>();
    int snameNotNull = 0;
    Set<Integer> snames = new HashSet<>();
    int phoneNotNull = 0;
    Set<String> phoneCodes = new HashSet<>();
    int mcount = 0;
    Set<Byte> country = new HashSet<>();
    int countryNotNull = 0;
    Set<Integer> city = new HashSet<>();
    int cityNotNull = 0;
    int[] statuses =new int[3];
    Set<Integer> interests = new HashSet<>();
    int premiumCount = 0;
    int likesCount = 0;


    public void analyze(Account account) {
        count++;
        minId = Math.min(minId, account.id);
        maxId = Math.max(maxId, account.id);
        int index = lastIndexOf(account.email, (byte) '@');
        emailsDomains.add(substring(account.email, index + 1));
        if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            fnameNotNull++;
            fnames.add(account.fname);
        }
        if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            snameNotNull++;
            snames.add(account.sname);
        }
        if (account.phone != null) {
            phoneNotNull++;
            int from = indexOf(account.phone, (byte) '(');
            int to = indexOf(account.phone, (byte) ')');
            if (from != -1 && to != -1) {
                phoneCodes.add(substring(account.phone, from + 1, to));
            }
        }
        if (account.sex) {
            mcount++;
        }
        if (account.country != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            countryNotNull++;
            country.add(account.country);
        }

        if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            cityNotNull++;
            city.add(account.city);
        }

        if (account.status == 0) {
            statuses[0]++;
        } else if (account.status == 1) {
            statuses[1]++;
        } else {
            statuses[2]++;
        }

        if (account.interests != null) {
            for (int i: account.interests) {
                interests.add(i);
            }
        }

        if (account.premiumStart != 0) {
            premiumCount++;
        }

        if (account.likes != null) {
            likesCount+= account.likes.length;
        }
    }

    private int lastIndexOf(byte[] values, byte ch) {

        for (int i = values.length - 1; i>=0; i--) {
            if (values[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    private int indexOf(byte[] values, byte ch) {

        for (int i = 0; i < values.length; i++) {
            if (values[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    private String substring(byte[] values, int from) {
        return substring(values, from, values.length);
    }

    private String substring(byte[] values, int from, int to) {
        byte[] result = new byte[to - from];
        for (int i = from; i < to; i++) {
            result[i - from] = values[i];
        }
        return new String(result);
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
