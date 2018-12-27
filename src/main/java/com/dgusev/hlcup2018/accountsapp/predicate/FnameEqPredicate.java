package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class FnameEqPredicate implements Predicate<Account> {

    private String fname;

    public FnameEqPredicate(String fname) {
        PredicateStatistics.fe.incrementAndGet();
        this.fname = fname;
    }

    @Override
    public boolean test(Account Account) {
        return Account.fname != null && Account.fname.equals(fname);
    }
}
