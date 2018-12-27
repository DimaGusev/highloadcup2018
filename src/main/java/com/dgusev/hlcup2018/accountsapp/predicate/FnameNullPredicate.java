package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class FnameNullPredicate implements Predicate<Account> {

    private int nill;

    public FnameNullPredicate(int nill) {
        PredicateStatistics.fn.incrementAndGet();
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.fname != null;
        } else  {
            return Account.fname == null;
        }
    }
}
