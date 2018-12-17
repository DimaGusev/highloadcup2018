package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class BirthLtPredicate implements Predicate<AccountDTO> {

    private int birth;

    public BirthLtPredicate(int birth) {
        PredicateStatistics.blt.incrementAndGet();
        this.birth = birth;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.birth < birth;
    }
}
