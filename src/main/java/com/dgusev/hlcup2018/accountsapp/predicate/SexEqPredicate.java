package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class SexEqPredicate implements Predicate<Account> {

    private String sex;

    public SexEqPredicate(String sex) {
        PredicateStatistics.sexe.incrementAndGet();
        this.sex = sex;
    }

    @Override
    public boolean test(Account Account) {
        return Account.sex.equals(sex);
    }

    public String getSex() {
        return sex;
    }
}
