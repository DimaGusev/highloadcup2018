package com.dgusev.hlcup2018.accountsapp.index;

import java.util.List;

public class OrIndexScan implements IndexScan {

    private IndexScan[] indexScans;
    private int[] indexes;
    private int[] minIds;
    private int prev = Integer.MAX_VALUE;

    public OrIndexScan(List<IndexScan> indexScans) {
        int count = indexScans.size();
        this.indexScans = indexScans.toArray(new IndexScan[count]);
        indexes = new int[count];
        minIds = new int[count];
        for (int i = 0; i < count; i++) {
            minIds[i] = -1;
        }
    }


    @Override
    public int getNext() {
        return -1;
    }

    private int getMax(int[] array) {
        int max = -1;
        for (int i = 0; i < array.length; i++) {
            if (array[i] > max) {
                max = array[i];
            }
        }
        return max;
    }
}
