package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class SnameStartsPredicate extends AbstractPredicate {

    public static final int ORDER = 27;

    private String start;
    private Dictionary dictionary;

    public SnameStartsPredicate setValue(String start, Dictionary dictionary) {
        this.start = start;
        this.dictionary = dictionary;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE && dictionary.getSname(account.sname).startsWith(start);
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
        return 0.015873;
    }

    @Override
    public double cost() {
        return 2.5;
    }
}
