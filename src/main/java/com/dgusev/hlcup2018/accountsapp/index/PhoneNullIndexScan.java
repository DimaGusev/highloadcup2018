package com.dgusev.hlcup2018.accountsapp.index;

public class PhoneNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public PhoneNullIndexScan(IndexHolder indexHolder, int nill) {
        super(indexHolder);
        if (nill == 1) {
            this.indexList = indexHolder.nullPhone;
        } else {
            this.indexList = indexHolder.notNullPhone;
        }

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
