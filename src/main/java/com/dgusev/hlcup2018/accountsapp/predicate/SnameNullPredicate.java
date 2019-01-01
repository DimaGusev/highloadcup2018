package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class SnameNullPredicate implements Predicate<Account> {

    private int nill;

    public SnameNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        } else  {
            return Account.sname == Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        }
    }

    public int getNill() {
        return nill;
    }
}
