package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CountryEqPredicate implements Predicate<Account> {

    private byte country;

    public CountryEqPredicate(byte country) {
        this.country = country;
    }

    @Override
    public boolean test(Account Account) {
        return Account.country == country;
    }

    public byte  getCounty() {
        return country;
    }
}
