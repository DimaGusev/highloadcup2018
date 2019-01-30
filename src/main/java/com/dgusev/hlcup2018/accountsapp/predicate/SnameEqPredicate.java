package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.SnameEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;


public class SnameEqPredicate extends AbstractPredicate {

    public static final int ORDER = 25;

    private int sname;

    public SnameEqPredicate setValue(int sname) {
        this.sname = sname;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE && account.sname != sname;
    }

    public int getSname() {
        return sname;
    }

    @Override
    public int getIndexCordiality() {
        return 800;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new SnameEqIndexScan(indexHolder, sname);
    }

    @Override
    public double probability() {
        return 0.0006;
    }

    @Override
    public double cost() {
        return 1;
    }
}
