package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class StatusEqIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public StatusEqIndexScan(IndexHolder indexHolder, String status) {
        super(indexHolder);
        if (status.equals("свободны")) {
            this.indexList = indexHolder.statusIndex.get(0);
        } else if (status.equals("всё сложно")) {
            this.indexList = indexHolder.statusIndex.get(1);
        } else {
            this.indexList = indexHolder.statusIndex.get(2);
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
