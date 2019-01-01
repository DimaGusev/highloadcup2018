package com.dgusev.hlcup2018.accountsapp.index;

public class SnameNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public SnameNullIndexScan(IndexHolder indexHolder, int nill) {
        super(indexHolder);
        if (nill == 1) {
            this.indexList = indexHolder.nullSname;
        } else {
            this.indexList = indexHolder.notNullSname;
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
