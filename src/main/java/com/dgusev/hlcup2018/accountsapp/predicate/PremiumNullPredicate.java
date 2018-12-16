package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class PremiumNullPredicate implements Predicate<AccountDTO> {

    private int nill;

    public PremiumNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (nill == 0) {
            return accountDTO.premiumStart != 0;
        } else  {
            return accountDTO.premiumStart == 0;
        }
    }
}
