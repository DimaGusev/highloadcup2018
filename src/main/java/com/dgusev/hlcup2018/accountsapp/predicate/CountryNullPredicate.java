package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class CountryNullPredicate implements Predicate<AccountDTO> {

    private int nill;

    public CountryNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (nill == 0) {
            return accountDTO.country != null;
        } else  {
            return accountDTO.country == null;
        }
    }
}
