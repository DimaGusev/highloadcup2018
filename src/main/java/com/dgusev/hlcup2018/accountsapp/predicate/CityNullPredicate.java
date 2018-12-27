package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class CityNullPredicate implements Predicate<Account> {

    private int nill;

    public CityNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(Account account) {
        if (nill == 0) {
            return account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        } else  {
            return account.city == Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        }
    }

    public int getNill() {
        return nill;
    }
}
