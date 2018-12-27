package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CountryEqPredicate implements Predicate<Account> {

    private String country;

    public CountryEqPredicate(String country) {
        PredicateStatistics.cne.incrementAndGet();
        this.country = country;
    }

    @Override
    public boolean test(Account Account) {
        return Account.country != null && Account.country.equals(country);
    }

    public String  getCounty() {
        return country;
    }
}
