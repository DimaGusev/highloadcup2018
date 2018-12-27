package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class SexEqIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public SexEqIndexScan(IndexHolder indexHolder, boolean sex) {
        super(indexHolder);
        this.indexList = indexHolder.sexIndex.get(sex ? (byte) 1: 0);
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
