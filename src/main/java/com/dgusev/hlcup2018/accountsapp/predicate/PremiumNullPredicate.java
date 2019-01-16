package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.PremiumNotNullIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNullPredicate extends AbstractPredicate {

    private int nill;

    public PremiumNullPredicate(int nill) {
        this.nill = nill;
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
}
