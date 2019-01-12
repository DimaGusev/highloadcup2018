package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class CountryNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public CountryNullIndexScan(IndexHolder indexHolder) {
        super(indexHolder);
        this.indexList = indexHolder.nullCountry;
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
