package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import gnu.trove.impl.Constants;

import java.util.List;
import java.util.function.Predicate;

public class FnameAnyPredicate implements Predicate<Account> {

    private int[] fnames;

    public FnameAnyPredicate(int[] fnames) {
        this.fnames = fnames;
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
}
