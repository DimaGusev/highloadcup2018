package com.dgusev.hlcup2018.accountsapp.index;

public class CityNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public CityNullIndexScan(IndexHolder indexHolder) {
        super(indexHolder);
        this.indexList = indexHolder.nullCity;
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
