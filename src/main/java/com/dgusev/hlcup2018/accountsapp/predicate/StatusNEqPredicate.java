package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class StatusNEqPredicate implements Predicate<Account> {

    private String status;

    public StatusNEqPredicate(String status) {
        PredicateStatistics.sne.incrementAndGet();
        this.status = status;
    }

    @Override
    public boolean test(Account Account) {
        return !Account.status.equals(status);
    }

    public String getStatus() {
        return status;
    }
}
