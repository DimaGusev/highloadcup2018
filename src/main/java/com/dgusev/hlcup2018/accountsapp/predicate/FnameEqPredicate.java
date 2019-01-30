package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.FnameEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class FnameEqPredicate extends AbstractPredicate {

    public static final int ORDER = 13;

    private int fname;

    public FnameEqPredicate setValue(int fname) {
        this.fname = fname;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE && account.fname == fname;
    }

    public int getFname() {
        return fname;
    }

    @Override
    public int getIndexCordiality() {
        return 12000;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new FnameEqIndexScan(indexHolder, fname);
    }

    @Override
    public double probability() {
        return 0.00926;
    }

    @Override
    public double cost() {
        return 1;
    }
}
