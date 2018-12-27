package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class StatusEqIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public StatusEqIndexScan(IndexHolder indexHolder, byte status) {
        super(indexHolder);
        this.indexList = indexHolder.statusIndex.get(status);
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
