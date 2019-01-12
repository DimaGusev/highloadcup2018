package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNowPredicate implements Predicate<Account> {


    @Override
    public boolean test(Account account) {
        return account.premium;
    }
}
