package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.List;
import java.util.function.Predicate;

public class CityAnyPredicate implements Predicate<AccountDTO> {

    private String[] cities;

    public CityAnyPredicate(String[] cities) {
        PredicateStatistics.cia.incrementAndGet();
        this.cities = cities;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.city != null && contains(cities, accountDTO.city);
    }

    private boolean contains(String[] arrray, String element) {
        for (int i = 0; i < arrray.length; i++) {
            if (arrray[i].equals(element)) {
                return true;
            }
        }
        return false;
    }

    public String[] getCities() {
        return cities;
    }
}
