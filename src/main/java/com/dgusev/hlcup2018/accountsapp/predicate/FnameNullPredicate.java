package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class FnameNullPredicate implements Predicate<Account> {

    private int nill;

    public FnameNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        } else  {
            return Account.fname == Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        }
    }
}
