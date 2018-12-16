package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class StatusEqPredicate implements Predicate<AccountDTO> {

    private String status;

    public StatusEqPredicate(String status) {
        this.status = status;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.status.equals(status);
    }
}
