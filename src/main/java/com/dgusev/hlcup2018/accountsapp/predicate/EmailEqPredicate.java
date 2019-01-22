package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.EmailEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.Arrays;
import java.util.function.Predicate;

public class EmailEqPredicate extends AbstractPredicate {

    private String email;
    private byte[] emailBytes;

    public EmailEqPredicate(String email) {
        this.email = email;
        this.emailBytes = email.getBytes();
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
}
