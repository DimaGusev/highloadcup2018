package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.PhoneEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.Predicate;

public class PhoneEqPredicate extends AbstractPredicate {

    public static final int ORDER = 20;

    private String phone;
    private byte[] phoneBytes;

    public PhoneEqPredicate setValue(String phone) {
        this.phone = phone;
        this.phoneBytes = phone.getBytes();
        return this;
    }

    @Override
    public boolean test(Account account) {
        return account.phone != null && Arrays.equals(account.phone, phoneBytes);
    }

    public String getPhone() {
        return phone;
    }

    @Override
    public int getIndexCordiality() {
        return 1;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new PhoneEqIndexScan(indexHolder, phone);
    }

    @Override
    public double probability() {
        return 0.0000007;
    }

    @Override
    public double cost() {
        return 2;
    }
}
