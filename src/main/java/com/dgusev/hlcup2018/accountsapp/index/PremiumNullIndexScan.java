package com.dgusev.hlcup2018.accountsapp.index;

public class PremiumNullIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public PremiumNullIndexScan(IndexHolder indexHolder, int nill) {
        super(indexHolder);
        if (nill == 1) {
            this.indexList = indexHolder.nullPremium;
        } else {
            this.indexList = indexHolder.notNullPremium;
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
