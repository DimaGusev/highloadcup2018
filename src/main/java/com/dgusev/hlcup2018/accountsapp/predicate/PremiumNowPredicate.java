package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.PremiumIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PremiumNowPredicate extends AbstractPredicate {

    public static final int ORDER = 22;

    @Override
    public boolean test(Account account) {
        return account.premium;
    }

    @Override
    public int getIndexCordiality() {
        return 204000;//TODO
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new PremiumIndexScan(indexHolder);
    }

    @Override
    public double probability() {
        //return 0.156923;
        return 0.127;
    }

    @Override
    public double cost() {
        return 1;
    }
}
