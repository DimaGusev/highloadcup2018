package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.InterestsContainsIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.List;
import java.util.function.Predicate;

public class InterestsContainsPredicate extends AbstractPredicate {

    public static final int ORDER = 16;

    private byte[] interests;

    public InterestsContainsPredicate setValue(byte[] interests) {
        this.interests = interests;
        return this;
    }

    @Override
    public boolean test(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            for (int i = 0; i < interests.length; i++) {
                if (!contains(account.interests, interests[i])) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public byte[] getInterests() {
        return interests;
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

    @Override
    public int getIndexCordiality() {
        if (interests.length == 1) {
            return 40000;
        } else {
            return (int) (40000 / Math.pow(3, interests.length));
        }
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new InterestsContainsIndexScan(indexHolder, interests);
    }

    @Override
    public double probability() {
        //return 0.03076923 / interests.length;
        return 0.02641;
    }

    @Override
    public double cost() {
        return 2.2;
    }
}
