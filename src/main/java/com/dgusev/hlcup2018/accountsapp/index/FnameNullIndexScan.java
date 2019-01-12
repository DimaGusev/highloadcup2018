package com.dgusev.hlcup2018.accountsapp.index;

public class FnameNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public FnameNullIndexScan(IndexHolder indexHolder) {
        super(indexHolder);
        this.indexList = indexHolder.nullFname;

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
