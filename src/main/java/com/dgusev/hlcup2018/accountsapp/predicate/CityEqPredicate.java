package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class CityEqPredicate implements Predicate<AccountDTO> {

    private String city;

    public CityEqPredicate(String city) {
        PredicateStatistics.cie.incrementAndGet();
        this.city = city;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.city != null && accountDTO.city.equals(city);
    }

    public String getCity() {
        return city;
    }
}
