package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class StatusNEqPredicate implements Predicate<AccountDTO> {

    private String status;

    public StatusNEqPredicate(String status) {
        PredicateStatistics.sne.incrementAndGet();
        this.status = status;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return !accountDTO.status.equals(status);
    }

    public String getStatus() {
        return status;
    }
}
