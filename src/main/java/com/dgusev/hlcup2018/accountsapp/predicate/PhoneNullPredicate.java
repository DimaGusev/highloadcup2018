package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class PhoneNullPredicate implements Predicate<AccountDTO> {

    private int nill;

    public PhoneNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (nill == 0) {
            return accountDTO.phone != null;
        } else  {
            return accountDTO.phone == null;
        }
    }
}
