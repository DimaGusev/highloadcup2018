package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailEqPredicate implements Predicate<Account> {

    private String email;

    public EmailEqPredicate(String email) {
        this.email = email;
    }

    @Override
    public boolean test(Account account) {
        return account.email.equals(email);
    }

    public String getEmail() {
        return email;
    }
}
