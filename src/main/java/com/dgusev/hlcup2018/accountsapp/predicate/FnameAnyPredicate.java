package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.FnameAnyIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.List;
import java.util.function.Predicate;

public class FnameAnyPredicate extends AbstractPredicate {

    public static final int ORDER = 12;

    private int[] fnames;

    public FnameAnyPredicate setValue(int[] fnames) {
        this.fnames = fnames;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE && contains(fnames, account.fname);
    }

    private boolean contains(int[] arrray, int element) {
        for (int i = 0; i < arrray.length; i++) {
            if (arrray[i] == element) {
                return true;
            }
        }
        return false;
    }

    public int[] getFnames() {
        return fnames;
    }

    @Override
    public int getIndexCordiality() {
        return 12000 * fnames.length;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new FnameAnyIndexScan(indexHolder, fnames);
    }

    @Override
    public double probability() {
       // return 0.00926 * fnames.length;
        return 0.034;
    }

    @Override
    public double cost() {
        return 1.5;
    }
}
