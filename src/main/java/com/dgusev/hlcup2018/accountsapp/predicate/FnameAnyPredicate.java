package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.List;
import java.util.function.Predicate;

public class FnameAnyPredicate implements Predicate<AccountDTO> {

    private List<String> fnames;

    public FnameAnyPredicate(List<String> fnames) {
        this.fnames = fnames;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.fname != null && fnames.contains(accountDTO.fname);
    }
}
