package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class SnameStartsPredicate implements Predicate<AccountDTO> {

    private String start;

    public SnameStartsPredicate(String start) {
        PredicateStatistics.sns.incrementAndGet();
        this.start = start;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.sname != null && accountDTO.sname.startsWith(start);
    }
}
