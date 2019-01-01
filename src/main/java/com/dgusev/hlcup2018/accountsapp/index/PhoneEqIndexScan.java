package com.dgusev.hlcup2018.accountsapp.index;

import gnu.trove.impl.Constants;

public class PhoneEqIndexScan extends AbstractIndexScan {

    private int accId;

    public PhoneEqIndexScan(IndexHolder indexHolder, String phone) {
        super(indexHolder);
        this.accId =  indexHolder.phoneIndex.get(phone);
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
