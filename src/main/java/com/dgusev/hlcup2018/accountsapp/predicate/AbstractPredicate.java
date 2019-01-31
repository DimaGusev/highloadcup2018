package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

public abstract class AbstractPredicate {

    public abstract int getIndexCordiality();

    public abstract IndexScan createIndexScan(IndexHolder indexHolder);

    public abstract double probability();

    public abstract double cost();

    public abstract boolean test(Account account);

    public double costScore() {
        return probability();
       // return probability() * cost();
    }
}
