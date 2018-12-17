package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class FnameNullPredicate implements Predicate<AccountDTO> {

    private int nill;

    public FnameNullPredicate(int nill) {
        PredicateStatistics.fn.incrementAndGet();
        this.nill = nill;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (nill == 0) {
            return accountDTO.fname != null;
        } else  {
            return accountDTO.fname == null;
        }
    }
}
