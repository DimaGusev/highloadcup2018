package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.PremiumNotNullIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNullPredicate extends AbstractPredicate {

    public static final int ORDER = 23;

    private int nill;

    public PremiumNullPredicate setValue(int nill) {
        this.nill = nill;
        return this;
    }

    @Override
    public boolean test(Account account) {
        if (nill == 0) {
            return account.premiumStart != 0;
        } else  {
            return account.premiumStart == 0;
        }
    }

    public int getNill() {
        return nill;
    }

    @Override
    public int getIndexCordiality() {
        if (nill == 0) {
            return 404000;
        } else  {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        if (nill == 0) {
            return new PremiumNotNullIndexScan(indexHolder);
        } else {
            return null;
        }
    }

    @Override
    public double probability() {
        /*if (nill == 0) {
            return 0.31;
        } else  {
            return 0.69;
        }*/
        return 0.45;
    }

    @Override
    public double cost() {
        return 1;
    }
}
