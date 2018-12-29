package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class CountryNullPredicate implements Predicate<Account> {

    private int nill;

    public CountryNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE;
        } else  {
            return Account.country == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE;
        }
    }

    public int getNill() {
        return nill;
    }
}
