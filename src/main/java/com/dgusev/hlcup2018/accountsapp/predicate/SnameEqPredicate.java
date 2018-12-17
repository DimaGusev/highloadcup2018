package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class SnameEqPredicate implements Predicate<AccountDTO> {

    private String sname;

    public SnameEqPredicate(String sname) {
        PredicateStatistics.sne.incrementAndGet();
        this.sname = sname;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.sname != null && accountDTO.sname.equals(sname);
    }
}
