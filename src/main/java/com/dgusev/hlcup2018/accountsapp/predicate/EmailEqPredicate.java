package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.EmailEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.Arrays;
import java.util.function.Predicate;

public class EmailEqPredicate extends AbstractPredicate {

    public static final int ORDER = 9;

    private String email;
    private byte[] emailBytes;

    public EmailEqPredicate setValue(String email) {
        this.email = email;
        this.emailBytes = email.getBytes();
        return this;
    }

    @Override
    public boolean test(Account account) {
        return Arrays.equals(account.email,emailBytes);
    }

    public String getEmail() {
        return email;
    }

    @Override
    public int getIndexCordiality() {
        return 1;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new EmailEqIndexScan(indexHolder, email);
    }

    @Override
    public double probability() {
        return 0.0000007;
    }

    @Override
    public double cost() {
        return 2;
    }
}
