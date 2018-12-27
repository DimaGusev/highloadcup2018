package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CityNullPredicate implements Predicate<Account> {

    private int nill;

    public CityNullPredicate(int nill) {
        PredicateStatistics.cin.incrementAndGet();
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.city != null;
        } else  {
            return Account.city == null;
        }
    }

    public int getNill() {
        return nill;
    }
}
