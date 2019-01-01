package com.dgusev.hlcup2018.accountsapp.index;

public class FnameEqIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public FnameEqIndexScan(IndexHolder indexHolder, int fname) {
        super(indexHolder);
        this.indexList = indexHolder.fnameIndex.get(fname);
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
