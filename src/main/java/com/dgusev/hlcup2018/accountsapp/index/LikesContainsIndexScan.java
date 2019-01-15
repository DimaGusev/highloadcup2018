package com.dgusev.hlcup2018.accountsapp.index;

import sun.misc.Unsafe;

public class LikesContainsIndexScan extends AbstractIndexScan {

    private static final Unsafe UNSAFE = com.dgusev.hlcup2018.accountsapp.service.Unsafe.UNSAFE;

    private long[] indexList;
    private long[] lengthList;
    private int[] indexes;
    private int[] state;
    private boolean alwaysFalse;

    public LikesContainsIndexScan(IndexHolder indexHolder, int[] likes) {
        super(indexHolder);
        int count = 0;
        for (int like: likes) {
            if (indexHolder.likesIndex[like] != 0) {
                count++;
            }
        }
        indexList = new long[count];
        lengthList = new long[count];
        index = 0;
        for (int like: likes) {
            if (indexHolder.likesIndex[like] != 0) {
                indexList[index] = indexHolder.likesIndex[like] + 1;
                lengthList[index] = UNSAFE.getByte(indexHolder.likesIndex[like]);
                index++;
            } else {
                alwaysFalse = true;
            }
        }
        indexes = new int[count];
        state = new int[count];
        for (int i = 0; i < count; i++) {
            if (lengthList[i] != 0) {
                indexes[i]++;
                state[i] = UNSAFE.getInt(indexList[i]);
                indexList[i]+=4;
            } else {
                state[i] = -1;
            }
        }
    }

    @Override
    public int getNext() {
        if (alwaysFalse) {
            return -1;
        }
        if (indexList != null && indexList.length != 0) {
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
                    for (int i = 0; i < indexList.length; i++) {
                        if (indexes[i] < lengthList[i]) {
                            indexes[i]++;
                            state[i] = UNSAFE.getInt(indexList[i]);
                            indexList[i]+=4;
                        } else {
                            state[i] = -1;
                        }
                    }
                    return result;
                }

                for (int i = 0; i < indexList.length; i++) {
                    if (state[i] != min) {
                        if (indexes[i] < lengthList[i]) {
                            indexes[i]++;
                            state[i] = UNSAFE.getInt(indexList[i]);
                            indexList[i]+=4;
                        } else {
                            state[i] = -1;
                        }
                        if (state[i] == -1) {
                            return -1;
                        }
                    }
                }
            }
        } else {
            return -1;
        }
    }

}
