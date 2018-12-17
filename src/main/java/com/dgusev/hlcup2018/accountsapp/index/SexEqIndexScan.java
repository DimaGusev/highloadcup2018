package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class SexEqIndexScan extends AbstractIndexScan {

    private List<Integer> indexList;

    public SexEqIndexScan(IndexHolder indexHolder, String sex) {
        super(indexHolder);
        this.indexList = indexHolder.sexIndex.get(sex);
    }

    @Override
    public int getNext() {
        if (indexList != null && index < indexList.size()) {
            return indexList.get(index++);
        } else {
            return -1;
        }
    }
}
