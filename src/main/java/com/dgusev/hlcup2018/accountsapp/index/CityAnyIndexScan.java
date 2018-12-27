package com.dgusev.hlcup2018.accountsapp.index;

public class CityAnyIndexScan extends AbstractIndexScan {

    private int[][] indexList;
    private int[] indexes;
    private int[] minIds;
    private int prev = Integer.MAX_VALUE;

    public CityAnyIndexScan(IndexHolder indexHolder, int[] cities) {
        super(indexHolder);
        int count = 0;
        for (int city: cities) {
            if (indexHolder.cityIndex.containsKey(city)) {
                count++;
            }
        }
        indexList = new int[count][];
        index = 0;
        for (int city: cities) {
            if (indexHolder.cityIndex.containsKey(city)) {
                indexList[index++] = indexHolder.cityIndex.get(city);
            }
        }
        indexes = new int[count];
        minIds = new int[count];
        for (int i = 0; i < count; i++) {
            minIds[i] = -1;
        }
    }

    @Override
    public int getNext() {
        if (indexList != null && indexList.length != 0) {
            for (int i = 0; i< indexList.length; i++) {
                minIds[i] =  -1;
                if (indexes[i] < indexList[i].length) {
                    for (int j = indexes[i]; j < indexList[i].length; j++) {
                        if (indexList[i][j] < prev) {
                            minIds[i] = indexList[i][j];
                            break;
                        }
                    }
                }
            }

            int max = getMax(minIds);
            if (max == -1) {
                return -1;
            }

            for (int i = 0; i< indexList.length; i++) {
                if (indexes[i] < indexList[i].length) {
                    for (int j = indexes[i]; j < indexList[i].length; j++) {
                        if (indexList[i][j] >= max) {
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
