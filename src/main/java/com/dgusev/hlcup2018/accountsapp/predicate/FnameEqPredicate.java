package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class FnameEqPredicate implements Predicate<Account> {

    private int fname;

    public FnameEqPredicate(int fname) {
        this.fname = fname;
    }

    @Override
    public boolean test(Account account) {
        return account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE && account.fname == fname;
    }
}
