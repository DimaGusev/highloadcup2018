package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CityEqPredicate implements Predicate<Account> {

    private String city;

    public CityEqPredicate(String city) {
        PredicateStatistics.cie.incrementAndGet();
        this.city = city;
    }

    @Override
    public boolean test(Account Account) {
        return Account.city != null && Account.city.equals(city);
    }

    public String getCity() {
        return city;
    }
}
