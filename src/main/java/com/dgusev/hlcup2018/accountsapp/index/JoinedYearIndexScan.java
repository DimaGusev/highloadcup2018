package com.dgusev.hlcup2018.accountsapp.index;

public class JoinedYearIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public JoinedYearIndexScan(IndexHolder indexHolder, int year) {
        super(indexHolder);
        this.indexList = indexHolder.joinedIndex.get(year);
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
