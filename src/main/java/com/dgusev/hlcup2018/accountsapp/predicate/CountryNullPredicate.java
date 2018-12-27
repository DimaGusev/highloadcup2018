package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CountryNullPredicate implements Predicate<Account> {

    private int nill;

    public CountryNullPredicate(int nill) {
        PredicateStatistics.cnn.incrementAndGet();
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.country != null;
        } else  {
            return Account.country == null;
        }
    }

    public int getNill() {
        return nill;
    }
}
