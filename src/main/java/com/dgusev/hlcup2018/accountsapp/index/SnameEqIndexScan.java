package com.dgusev.hlcup2018.accountsapp.index;

public class SnameEqIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public SnameEqIndexScan(IndexHolder indexHolder, int sname) {
        super(indexHolder);
        this.indexList = indexHolder.snameIndex.get(sname);
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
