package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.FnameNullIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class FnameNullPredicate extends AbstractPredicate {

    private int nill;

    public FnameNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        } else  {
            return Account.fname == Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        }
    }

    public int getNill() {
        return nill;
    }

    @Override
    public int getIndexCordiality() {
        if (nill == 1) {
            return 120000;
        } else  {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        if (nill == 1) {
            return new FnameNullIndexScan(indexHolder);
        } else {
            return null;
        }
    }
}
