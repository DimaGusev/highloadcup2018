package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.EmailEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailEqPredicate extends AbstractPredicate {

    private String email;

    public EmailEqPredicate(String email) {
        this.email = email;
    }

    @Override
    public boolean test(Account account) {
        return account.email.equals(email);
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
