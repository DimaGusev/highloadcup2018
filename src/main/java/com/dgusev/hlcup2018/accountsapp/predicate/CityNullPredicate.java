package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.CityNullIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class CityNullPredicate extends AbstractPredicate {

    public static final int ORDER = 5;

    private int nill;

    public CityNullPredicate setValue(int nill) {
        this.nill = nill;
        return this;
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

    @Override
    public int getIndexCordiality() {
        if (nill == 1) {
            return 505000;
        } else  {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        if (nill == 1) {
            return new CityNullIndexScan(indexHolder);
        } else {
            return null;
        }
    }

    @Override
    public double probability() {
        /*if (nill == 1) {
            return 0.388;
        } else {
            return 0.612;
        }*/
        return 0.47;
    }

    @Override
    public double cost() {
        return 1;
    }
}
