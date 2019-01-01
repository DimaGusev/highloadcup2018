package com.dgusev.hlcup2018.accountsapp.index;

import gnu.trove.impl.Constants;

import java.util.Arrays;

public class EmailEqIndexScan extends AbstractIndexScan {

    private int accId;

    public EmailEqIndexScan(IndexHolder indexHolder, String email) {
        super(indexHolder);
        this.accId =  indexHolder.emailIndex.get(email);
    }

    @Override
    public int getNext() {
        if (accId != -1 && accId != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            int result = accId;
            accId = -1;
            return result;
        } else  {
            return -1;
        }
    }
}
