package com.dgusev.hlcup2018.accountsapp.index;

public class ArrayIndexScan implements IndexScan {

    private int[] array;
    private int index;

    public ArrayIndexScan(int[] array) {
        this.array = array;
    }

    @Override
    public int getNext() {
        if (index < array.length) {
            return array[index++];
        } else {
            return -1;
        }
    }
}
