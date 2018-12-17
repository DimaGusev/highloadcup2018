package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class CountryEqPredicate implements Predicate<AccountDTO> {

    private String country;

    public CountryEqPredicate(String country) {
        PredicateStatistics.cne.incrementAndGet();
        this.country = country;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.country != null && accountDTO.country.equals(country);
    }

    public String  getCounty() {
        return country;
    }
}
