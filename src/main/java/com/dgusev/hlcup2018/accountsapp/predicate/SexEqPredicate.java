package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class SexEqPredicate extends AbstractPredicate {

    public static final int ORDER = 24;

    private boolean sex;

    public SexEqPredicate setValue(boolean sex) {
        this.sex = sex;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return account.sex == sex;
    }

    public boolean getSex() {
        return sex;
    }

    @Override
    public int getIndexCordiality() {
        return Integer.MAX_VALUE;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return null;
    }

    @Override
    public double probability() {
        return 0.5;
    }

    @Override
    public double cost() {
        return 1;
    }
}
