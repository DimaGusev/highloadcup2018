package com.dgusev.hlcup2018.accountsapp.index;

public class ArrayIndexScan implements IndexScan {

    private int[] array;
    private int index;
    private int to;

    public ArrayIndexScan(int[] array) {
        this(array, 0, array.length);
    }

    public ArrayIndexScan(int[] array, int from, int to) {
        this.array = array;
        index = from;
        this.to = to;
    }

    @Override
    public int getNext() {
        if (index < to) {
            return array[index++];
        } else {
            return -1;
        }
    }
}
