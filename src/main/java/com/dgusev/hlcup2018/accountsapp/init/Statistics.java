package com.dgusev.hlcup2018.accountsapp.init;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
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
        int index = account.email.lastIndexOf('@');
        emailsDomains.add(account.email.substring(index + 1));
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
            int from = account.phone.indexOf('(');
            int to = account.phone.indexOf(')');
            if (from != -1 && to != -1) {
                phoneCodes.add(account.phone.substring(from + 1, to));
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
