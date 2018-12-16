package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.Date;
import java.util.function.Predicate;

public class BirthYearPredicate implements Predicate<AccountDTO> {

    private int year;

    public BirthYearPredicate(int birth) {
        this.year = birth;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return new Date(accountDTO.birth * 1000L).getYear() + 1900 == year;
    }
}
