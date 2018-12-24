package com.dgusev.hlcup2018.accountsapp.index;

public class PremiumIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public PremiumIndexScan(IndexHolder indexHolder) {
        super(indexHolder);
        this.indexList = indexHolder.premiumIndex;
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
