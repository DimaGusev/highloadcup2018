package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class StatusEqPredicate implements Predicate<Account> {

    private String status;

    public StatusEqPredicate(String status) {
        PredicateStatistics.ste.incrementAndGet();
        this.status = status;
    }

    @Override
    public boolean test(Account Account) {
        return Account.status.equals(status);
    }

    public String getStatus() {
        return status;
    }
}
