package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.CityEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CityEqPredicate extends AbstractPredicate {

    private int city;

    public CityEqPredicate(int city) {
        this.city = city;
    }

    @Override
    public boolean test(Account account) {
        return account.city == city;
    }

    public int getCity() {
        return city;
    }

    @Override
    public int getIndexCordiality() {
        return 2200;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new CityEqIndexScan(indexHolder, city);
    }
}
