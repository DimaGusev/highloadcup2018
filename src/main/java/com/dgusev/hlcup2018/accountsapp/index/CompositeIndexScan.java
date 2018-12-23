package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.service.AccountService;

import java.util.Arrays;
import java.util.List;

public class CompositeIndexScan implements IndexScan {

    private IndexScan[] indexScans;
    private int[] state;

    public CompositeIndexScan(List<IndexScan> indexScans) {
        this.indexScans = indexScans.toArray(new IndexScan[indexScans.size()]);
        state = new int[indexScans.size()];
        for (int i = 0; i < indexScans.size(); i++) {
            state[i] = indexScans.get(i).getNext();
        }
    }

    public int getNext() {
        while (true) {
            boolean equals = true;
            int min = Integer.MAX_VALUE;
            int prev = -1;
            for (int i = 0; i< state.length; i++) {
                if (state[i] < min) {
                    min = state[i];
                }
                if (prev != -1) {
                    if (state[i] != prev) {
                        equals = false;
                    }
                }
                prev = state[i];
            }

            if (equals) {
                int result = state[0];
                for (int i = 0; i < indexScans.length; i++) {
                    state[i] = indexScans[i].getNext();
                }
                return result;
            }

            for (int i = 0; i < indexScans.length; i++) {
                if (state[i] != min) {
                    state[i] = indexScans[i].getNext();
                    if (state[i] == -1) {
                        return -1;
                    }
                }
            }
        }
    }


}
