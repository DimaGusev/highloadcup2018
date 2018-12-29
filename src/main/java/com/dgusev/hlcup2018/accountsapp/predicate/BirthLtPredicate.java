package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class BirthLtPredicate implements Predicate<Account> {

    private int birth;

    public BirthLtPredicate(int birth) {
        this.birth = birth;
    }

    @Override
    public boolean test(Account Account) {
        return Account.birth < birth;
    }
}
