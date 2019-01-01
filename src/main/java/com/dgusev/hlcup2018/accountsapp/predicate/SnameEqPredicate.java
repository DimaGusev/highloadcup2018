package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class SnameEqPredicate implements Predicate<Account> {

    private int sname;

    public SnameEqPredicate(int sname) {
        this.sname = sname;
    }

    @Override
    public boolean test(Account account) {
        return account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE && account.sname != sname;
    }

    public int getSname() {
        return sname;
    }
}
