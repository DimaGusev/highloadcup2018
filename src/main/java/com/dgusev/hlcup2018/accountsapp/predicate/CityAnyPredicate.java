package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.List;
import java.util.function.Predicate;

public class CityAnyPredicate implements Predicate<AccountDTO> {

    private List<String> cities;

    public CityAnyPredicate(List<String> cities) {
        PredicateStatistics.cia.incrementAndGet();
        this.cities = cities;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.city != null && cities.contains(accountDTO.city);
    }
}
