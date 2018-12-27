package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNullPredicate implements Predicate<Account> {

    private int nill;

    public PremiumNullPredicate(int nill) {
        PredicateStatistics.prnu.incrementAndGet();
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.premiumStart != 0;
        } else  {
            return Account.premiumStart == 0;
        }
    }
}
