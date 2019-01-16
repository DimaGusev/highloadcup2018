package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.SnameEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;


public class SnameEqPredicate extends AbstractPredicate {

    private int sname;

    public SnameEqPredicate(int sname) {
        this.sname = sname;
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
}
