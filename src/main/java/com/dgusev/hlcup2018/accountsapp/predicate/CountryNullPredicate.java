package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.CityNullIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.CountryNullIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class CountryNullPredicate extends AbstractPredicate {

    public static final int ORDER = 7;

    private int nill;

    public CountryNullPredicate setValue(int nill) {
        this.nill = nill;
        return this;
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

    @Override
    public int getIndexCordiality() {
        if (nill == 1) {
            return 250000;
        } else  {
            return Integer.MAX_VALUE;
        }    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        if (nill == 1) {
            return new CountryNullIndexScan(indexHolder);
        } else {
            return null;
        }
    }

    @Override
    public double probability() {
        /*if (nill == 1) {
            return 0.1923;
        } else {
            return 0.8077;
        }*/
        return 0.3;

    }

    @Override
    public double cost() {
        return 1;
    }
}
