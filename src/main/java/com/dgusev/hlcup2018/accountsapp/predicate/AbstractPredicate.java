package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public abstract class AbstractPredicate implements Predicate<Account> {

    public abstract int getIndexCordiality();

    public abstract IndexScan createIndexScan(IndexHolder indexHolder);
}
