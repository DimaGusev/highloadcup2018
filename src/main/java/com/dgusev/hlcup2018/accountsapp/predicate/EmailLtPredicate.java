package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class EmailLtPredicate implements Predicate<AccountDTO> {

    private String email;

    public EmailLtPredicate(String email) {
        this.email = email;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.email.compareTo(email) < 0;
    }
}
