package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class StatusEqPredicate implements Predicate<Account> {

    private byte status;

    public StatusEqPredicate(byte status) {
        this.status = status;
    }

    @Override
    public boolean test(Account account) {
        return account.status == status;
    }

    public byte getStatus() {
        return status;
    }
}
