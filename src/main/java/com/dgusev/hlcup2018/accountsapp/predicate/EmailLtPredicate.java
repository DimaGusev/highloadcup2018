package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailLtPredicate implements Predicate<Account> {

    private String email;

    public EmailLtPredicate(String email) {
        PredicateStatistics.elt.incrementAndGet();
        this.email = email;
    }

    @Override
    public boolean test(Account Account) {
        return Account.email.compareTo(email) < 0;
    }
}
