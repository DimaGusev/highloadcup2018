package com.dgusev.hlcup2018.accountsapp.index;

public class PhoneCodeIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public PhoneCodeIndexScan(IndexHolder indexHolder, String code) {
        super(indexHolder);
        this.indexList = indexHolder.phoneCodeIndex.get(code);
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
