package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNowPredicate implements Predicate<Account> {

    private int now;

    public PremiumNowPredicate(int now) {
        PredicateStatistics.prn.incrementAndGet();
        this.now = now;
    }

    @Override
    public boolean test(Account Account) {
        return Account.premiumStart != 0 && Account.premiumStart <= now && (Account.premiumFinish == 0 || Account.premiumFinish > now);
    }
}
