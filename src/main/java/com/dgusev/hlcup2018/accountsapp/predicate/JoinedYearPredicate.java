package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.Date;
import java.util.function.Predicate;

public class JoinedYearPredicate implements Predicate<AccountDTO> {

    private int year;

    public JoinedYearPredicate(int birth) {
        this.year = birth;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return new Date(accountDTO.joined * 1000L).getYear() + 1900 == year;
    }
}
