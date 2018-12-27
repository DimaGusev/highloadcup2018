package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CityEqPredicate implements Predicate<Account> {

    private int city;

    public CityEqPredicate(int city) {
        this.city = city;
    }

    @Override
    public boolean test(Account account) {
        return account.city == city;
    }

    public int getCity() {
        return city;
    }
}
