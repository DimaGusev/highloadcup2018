package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class PremiumNowPredicate implements Predicate<AccountDTO> {

    private int now;

    public PremiumNowPredicate(int now) {
        this.now = now;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.premiumStart != 0 && accountDTO.premiumStart <= now && (accountDTO.premiumFinish == 0 || accountDTO.premiumFinish > now);
    }
}
