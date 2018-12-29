package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailEqPredicate implements Predicate<Account> {

    private String email;

    public EmailEqPredicate(String email) {
        this.email = email;
    }

    @Override
    public boolean test(Account Account) {
        return Account.email.equals(email);
    }
}
