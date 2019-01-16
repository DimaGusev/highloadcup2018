package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class StatusEqPredicate extends AbstractPredicate {

    private byte status;

    public StatusEqPredicate(byte status) {
        this.status = status;
    }

    @Override
    public boolean test(Account account) {
        return account.status == status;
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
}
