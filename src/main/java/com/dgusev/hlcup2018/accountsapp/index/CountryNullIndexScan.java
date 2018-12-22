package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class CountryNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public CountryNullIndexScan(IndexHolder indexHolder, int nill) {
        super(indexHolder);
        if (nill == 1) {
            this.indexList = indexHolder.nullCountry;
        } else {
            this.indexList = indexHolder.notNullCountry;
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
