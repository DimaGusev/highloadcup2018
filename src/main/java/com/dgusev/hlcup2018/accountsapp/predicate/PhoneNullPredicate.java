package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.PhoneNotNullIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PhoneNullPredicate extends AbstractPredicate {

    public static final int ORDER = 21;

    private int nill;

    public PhoneNullPredicate setValue(int nill) {
        this.nill = nill;
        return this;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.phone != null;
        } else  {
            return Account.phone == null;
        }
    }

    public int getNill() {
        return nill;
    }

    @Override
    public int getIndexCordiality() {
        if (nill == 0) {
            return 533000;
        } else  {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        if (nill == 0) {
            return new PhoneNotNullIndexScan(indexHolder);
        } else  {
            return null;
        }
    }

    @Override
    public double probability() {
        /*if (nill == 0) {
            return 0.41;
        } else  {
            return 0.59;
        }*/
        return 0.4875;
    }

    @Override
    public double cost() {
        return 1;
    }
}
