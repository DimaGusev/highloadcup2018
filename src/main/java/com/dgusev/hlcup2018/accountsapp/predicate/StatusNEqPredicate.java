package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class StatusNEqPredicate extends AbstractPredicate {

    public static final int ORDER = 29;

    private byte status;

    public StatusNEqPredicate setValue(byte status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return account.status != status;
    }

    public byte getStatus() {
        return status;
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
        return 0.67;
    }

    @Override
    public double cost() {
        return 1;
    }
}
