package com.dgusev.hlcup2018.accountsapp.index;

public class CityEqIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public CityEqIndexScan(IndexHolder indexHolder, String city) {
        super(indexHolder);
        this.indexList = indexHolder.cityIndex.get(city);
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
