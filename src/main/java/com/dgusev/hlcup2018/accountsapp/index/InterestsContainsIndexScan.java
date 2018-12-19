package com.dgusev.hlcup2018.accountsapp.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InterestsContainsIndexScan extends AbstractIndexScan {

    private List<List<Integer>> indexList;
    private int[] indexes;
    private List<Integer> minIds;
    private int prev = Integer.MAX_VALUE;

    public InterestsContainsIndexScan(IndexHolder indexHolder, List<String> interests) {
        super(indexHolder);
        indexList = new ArrayList<>();
        for (String interes: interests) {
            if (indexHolder.interestsIndex.containsKey(interes)) {
                indexList.add(indexHolder.interestsIndex.get(interes));
            }
        }
        indexes = new int[indexList.size()];
        minIds = new ArrayList<>();
        for (int i = 0; i < indexList.size(); i++) {
            minIds.add(-1);
        }
    }

    @Override
    public int getNext() {
        if (indexList != null && !indexList.isEmpty()) {
            for (int i = 0; i< indexList.size(); i++) {
                minIds.set(i, -1);
                if (indexes[i] < indexList.get(i).size()) {
                    for (int j = indexes[i]; j < indexList.get(i).size(); j++) {
                        if (indexList.get(i).get(j) < prev) {
                            minIds.set(i, indexList.get(i).get(j));
                            break;
                        }
                    }
                }
            }

            int max = Collections.max(minIds);
            if (max == -1) {
                return -1;
            }

            for (int i = 0; i< indexList.size(); i++) {
                if (indexes[i] < indexList.get(i).size()) {
                    for (int j = indexes[i]; j < indexList.get(i).size(); j++) {
                        if (indexList.get(i).get(j) >= max) {
                            indexes[i]++;
                        } else {
                            break;
                        }
                    }
                }
            }
            prev = max;
            return max;
        } else {
            return -1;
        }
    }
}
