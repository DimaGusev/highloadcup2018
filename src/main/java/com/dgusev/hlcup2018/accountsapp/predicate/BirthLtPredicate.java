package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class BirthLtPredicate extends AbstractPredicate {

    public static final int ORDER = 1;

    private int birth;

    public BirthLtPredicate setValue(int birth) {
        this.birth = birth;
        return this;
    }

    @Override
    public boolean test(Account Account) {
        return Account.birth < birth;
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
        return 0.18;
    }

    @Override
    public double cost() {
        return 1;
    }

}
