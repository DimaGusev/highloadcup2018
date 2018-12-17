package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class CountryEqIndexScan extends AbstractIndexScan {

    private List<Integer> indexList;

    public CountryEqIndexScan(IndexHolder indexHolder, String country) {
        super(indexHolder);
        this.indexList = indexHolder.countryIndex.get(country);
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
