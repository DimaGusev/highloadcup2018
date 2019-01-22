package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailGtPredicate extends AbstractPredicate {

    private String email;
    private byte[] emailBytes;

    public EmailGtPredicate(String email) {
        this.email = email;
        this.emailBytes = email.getBytes();
    }

    @Override
    public boolean test(Account account) {
        return compareTo(account.email, emailBytes) > 0;
    }

    private int compareTo(byte[] values1, byte[] values2) {
        int len1 = values1.length;
        int len2 = values2.length;
        int lim = 0;
        if (len1 < len2) {
            lim = len1;
        } else {
            lim = len2;
        }
        int k = 0;
        while (k < lim) {
            byte c1 = values1[k];
            byte c2 = values2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }

    @Override
    public int getIndexCordiality() {
        return Integer.MAX_VALUE;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return null;
    }
}
