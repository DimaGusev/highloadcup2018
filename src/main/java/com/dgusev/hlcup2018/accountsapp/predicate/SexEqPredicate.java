package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class SexEqPredicate implements Predicate<Account> {

    private boolean sex;

    public SexEqPredicate(boolean sex) {
        this.sex = sex;
    }

    @Override
    public boolean test(Account account) {
        return account.sex == sex;
    }

    public boolean getSex() {
        return sex;
    }
}
