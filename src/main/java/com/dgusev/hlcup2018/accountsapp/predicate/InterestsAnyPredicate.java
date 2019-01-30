package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.InterestsAnyIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.List;
import java.util.function.Predicate;

public class InterestsAnyPredicate extends AbstractPredicate {

    public static final int ORDER = 15;

    private byte[] interests;

    public InterestsAnyPredicate setValue(byte[] interests) {
        this.interests = interests;
        return this;
    }

    @Override
    public boolean test(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            for (int i = 0; i < interests.length; i++) {
                if (contains(account.interests, interests[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    private boolean contains(byte[] arrray, byte element) {
        for (int i = 0; i < arrray.length; i++) {
            if (arrray[i] == element) {
                return true;
            }
            if (arrray[i] > element) {
                return false;
            }
        }
        return false;
    }

    public byte[] getInterests() {
        return interests;
    }

    @Override
    public int getIndexCordiality() {
        return 40000* interests.length;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new InterestsAnyIndexScan(indexHolder, interests);
    }

    @Override
    public double probability() {
        //0.03076923 * interests.length
        return 0.0719;
    }

    @Override
    public double cost() {
        return 2.5;
    }
}
