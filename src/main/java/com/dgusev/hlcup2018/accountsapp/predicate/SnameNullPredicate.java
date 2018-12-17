package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class SnameNullPredicate implements Predicate<AccountDTO> {

    private int nill;

    public SnameNullPredicate(int nill) {
        PredicateStatistics.snn.incrementAndGet();
        this.nill = nill;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (nill == 0) {
            return accountDTO.sname != null;
        } else  {
            return accountDTO.sname == null;
        }
    }
}
