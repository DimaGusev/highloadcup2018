package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class SexEqPredicate implements Predicate<AccountDTO> {

    private String sex;

    public SexEqPredicate(String sex) {
        PredicateStatistics.sexe.incrementAndGet();
        this.sex = sex;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.sex.equals(sex);
    }

    public String getSex() {
        return sex;
    }
}
