package com.dgusev.hlcup2018.accountsapp.index;

public class PhoneNotNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public PhoneNotNullIndexScan(IndexHolder indexHolder) {
        super(indexHolder);
        this.indexList = indexHolder.notNullPhone;

    }

    @Override
    public int getNext() {
        if (indexList != null && index < indexList.length) {
            return indexList[index++];
        } else {
            return -1;
        }
    }
}
