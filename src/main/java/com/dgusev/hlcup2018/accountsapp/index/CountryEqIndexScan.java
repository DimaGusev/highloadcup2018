package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class CountryEqIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public CountryEqIndexScan(IndexHolder indexHolder, byte country) {
        super(indexHolder);
        this.indexList = indexHolder.countryIndex.get(country);
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
