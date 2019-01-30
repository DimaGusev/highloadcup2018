package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.PhoneCodeIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PhoneCodePredicate extends AbstractPredicate {

    public static final int ORDER = 19;

    private String code;

    public PhoneCodePredicate setValue(String code) {
        this.code = code;
        return this;
    }

    @Override
    public boolean test(Account account) {
        if (account.phone == null) {
            return false;
        }
        int open = indexOf(account.phone, (byte) '(');
        if (open == -1) {
            return false;
        }
        if (open + code.length() > account.phone.length) {
            return false;
        }
        if (account.phone[open + code.length() + 1] != ')') {
            return false;
        }
        for (int i = 0; i < code.length(); i++) {
            if (account.phone[open + i + 1] != code.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private int indexOf(byte[] values, byte ch) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    public String getCode() {
        return code;
    }

    @Override
    public int getIndexCordiality() {
        return 13000;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new PhoneCodeIndexScan(indexHolder, code);
    }

    @Override
    public double probability() {
        //0.0041;
        return 0.001;
    }

    @Override
    public double cost() {
        return 2.8;
    }
}
