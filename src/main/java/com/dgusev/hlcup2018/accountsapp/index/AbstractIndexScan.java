package com.dgusev.hlcup2018.accountsapp.index;

public abstract class AbstractIndexScan implements IndexScan {
    protected int index = 0;
    protected IndexHolder indexHolder;
    public AbstractIndexScan(IndexHolder indexHolder) {
        this.indexHolder = indexHolder;
    }
}
