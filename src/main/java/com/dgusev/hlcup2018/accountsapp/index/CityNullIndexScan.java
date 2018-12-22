package com.dgusev.hlcup2018.accountsapp.index;

public class CityNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public CityNullIndexScan(IndexHolder indexHolder, int nill) {
        super(indexHolder);
        if (nill == 1) {
            this.indexList = indexHolder.nullCity;
        } else {
            this.indexList = indexHolder.notNullCity;
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
