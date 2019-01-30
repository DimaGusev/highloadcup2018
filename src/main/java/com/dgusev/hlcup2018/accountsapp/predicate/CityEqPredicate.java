package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.CityEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CityEqPredicate extends AbstractPredicate {


    public static final int ORDER = 4;

    private int city;

    public CityEqPredicate setValue(int city) {
        this.city = city;
        return this;
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

    @Override
    public double probability() {
        //0.00165
        return 0.0048951;
    }

    @Override
    public double cost() {
        return 1;
    }
}
