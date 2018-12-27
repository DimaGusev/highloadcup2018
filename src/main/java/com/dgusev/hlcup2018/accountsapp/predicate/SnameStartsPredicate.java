package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class SnameStartsPredicate implements Predicate<Account> {

    private String start;

    public SnameStartsPredicate(String start) {
        PredicateStatistics.sns.incrementAndGet();
        this.start = start;
    }

    @Override
    public boolean test(Account Account) {
        return Account.sname != null && Account.sname.startsWith(start);
    }
}
