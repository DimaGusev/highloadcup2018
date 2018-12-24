package com.dgusev.hlcup2018.accountsapp.index;

public class EmailDomainIndexScan extends AbstractIndexScan {

    private int[] indexList;

    public EmailDomainIndexScan(IndexHolder indexHolder, String domain) {
        super(indexHolder);
        this.indexList = indexHolder.emailDomainIndex.get(domain);
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
