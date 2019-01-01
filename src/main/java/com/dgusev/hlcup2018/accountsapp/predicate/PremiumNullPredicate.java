package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNullPredicate implements Predicate<Account> {

    private int nill;

    public PremiumNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(Account account) {
        if (nill == 0) {
            return account.premiumStart != 0;
        } else  {
            return account.premiumStart == 0;
        }
    }

    public int getNill() {
        return nill;
    }
}
