package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailGtPredicate implements Predicate<Account> {

    private String email;

    public EmailGtPredicate(String email) {
        PredicateStatistics.egt.incrementAndGet();
        this.email = email;
    }

    @Override
    public boolean test(Account Account) {
        return Account.email.compareTo(email) > 0;
    }
}
