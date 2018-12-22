package com.dgusev.hlcup2018.accountsapp.index;

public class BirthYearIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public BirthYearIndexScan(IndexHolder indexHolder, int year) {
        super(indexHolder);
        this.indexList = indexHolder.birthYearIndex.get(year);
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
