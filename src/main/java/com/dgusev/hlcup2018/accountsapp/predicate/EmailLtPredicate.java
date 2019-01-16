package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailLtPredicate extends AbstractPredicate {

    private String email;

    public EmailLtPredicate(String email) {
        this.email = email;
    }

    @Override
    public boolean test(Account Account) {
        return Account.email.compareTo(email) < 0;
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
