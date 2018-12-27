package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.List;
import java.util.function.Predicate;

public class CityAnyPredicate implements Predicate<Account> {

    private int[] cities;

    public CityAnyPredicate(int[] cities) {
        this.cities = cities;
    }

    @Override
    public boolean test(Account account) {
        return account.city != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE && contains(cities, account.city);
    }

    private boolean contains(int[] arrray, int element) {
        for (int i = 0; i < arrray.length; i++) {
            if (arrray[i] == element) {
                return true;
            }
        }
        return false;
    }

    public int[] getCities() {
        return cities;
    }
}
