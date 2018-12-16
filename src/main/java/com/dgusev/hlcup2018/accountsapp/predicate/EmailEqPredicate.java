package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class EmailEqPredicate implements Predicate<AccountDTO> {

    private String email;

    public EmailEqPredicate(String email) {
        this.email = email;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.email.equals(email);
    }
}
