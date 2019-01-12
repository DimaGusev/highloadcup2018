package com.dgusev.hlcup2018.accountsapp.index;

public class PremiumNotNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public PremiumNotNullIndexScan(IndexHolder indexHolder) {
        super(indexHolder);
        this.indexList = indexHolder.notNullPremium;
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
