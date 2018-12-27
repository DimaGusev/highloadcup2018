package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PhoneEqPredicate implements Predicate<Account> {

    private String phone;

    public PhoneEqPredicate(String phone) {
        PredicateStatistics.pe.incrementAndGet();
        this.phone = phone;
    }

    @Override
    public boolean test(Account Account) {
        return Account.phone != null && Account.phone.equals(phone);
    }
}
