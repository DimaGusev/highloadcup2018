package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class BirthGtPredicate extends AbstractPredicate {

    private int birth;

    public BirthGtPredicate(int birth) {
        this.birth = birth;
    }

    @Override
    public boolean test(Account Account) {
        return Account.birth > birth;
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
