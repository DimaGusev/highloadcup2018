package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.PhoneEqIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PhoneEqPredicate extends AbstractPredicate {

    private String phone;

    public PhoneEqPredicate(String phone) {
        this.phone = phone;
    }

    @Override
    public boolean test(Account account) {
        return account.phone != null && account.phone.equals(phone);
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
}
