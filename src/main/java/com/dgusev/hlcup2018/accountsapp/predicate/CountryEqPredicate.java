package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.CountryEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class CountryEqPredicate extends AbstractPredicate {


    public static final int ORDER = 6;

    private byte country;

    public CountryEqPredicate setValue(byte country) {
        this.country = country;
        return this;
    }

    @Override
    public boolean test(Account Account) {
        return Account.country == country;
    }

    public byte getCounty() {
        return country;
    }

    @Override
    public int getIndexCordiality() {
        return 19000;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new CountryEqIndexScan(indexHolder, country);
    }

    @Override
    public double probability() {
        //0.0143
        return 0.017;
    }

    @Override
    public double cost() {
        return 1;
    }
}
