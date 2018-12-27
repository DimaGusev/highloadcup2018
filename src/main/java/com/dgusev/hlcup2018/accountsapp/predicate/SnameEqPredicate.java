package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class SnameEqPredicate implements Predicate<Account> {

    private String sname;

    public SnameEqPredicate(String sname) {
        PredicateStatistics.sne.incrementAndGet();
        this.sname = sname;
    }

    @Override
    public boolean test(Account Account) {
        return Account.sname != null && Account.sname.equals(sname);
    }
}
