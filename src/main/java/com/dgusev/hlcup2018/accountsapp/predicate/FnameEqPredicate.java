package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class FnameEqPredicate implements Predicate<AccountDTO> {

    private String fname;

    public FnameEqPredicate(String fname) {
        this.fname = fname;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.fname != null && accountDTO.fname.equals(fname);
    }
}
