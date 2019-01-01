package com.dgusev.hlcup2018.accountsapp.index;

public class FnameNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public FnameNullIndexScan(IndexHolder indexHolder, int nill) {
        super(indexHolder);
        if (nill == 1) {
            this.indexList = indexHolder.nullFname;
        } else {
            this.indexList = indexHolder.notNullFname;
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
