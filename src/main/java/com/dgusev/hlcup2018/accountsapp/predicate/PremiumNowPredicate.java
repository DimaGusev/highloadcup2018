package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNowPredicate implements Predicate<Account> {

    private int now;

    public PremiumNowPredicate(int now) {
        this.now = now;
    }

    @Override
    public boolean test(Account account) {
        return account.premiumStart != 0 && account.premiumStart <= now && (account.premiumFinish == 0 || account.premiumFinish > now);
    }
}
