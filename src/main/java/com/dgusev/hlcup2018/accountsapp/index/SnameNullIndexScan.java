package com.dgusev.hlcup2018.accountsapp.index;

public class SnameNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public SnameNullIndexScan(IndexHolder indexHolder) {
        super(indexHolder);
        this.indexList = indexHolder.nullSname;

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
