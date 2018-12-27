package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class SnameNullPredicate implements Predicate<Account> {

    private int nill;

    public SnameNullPredicate(int nill) {
        PredicateStatistics.snn.incrementAndGet();
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.sname != null;
        } else  {
            return Account.sname == null;
        }
    }
}
