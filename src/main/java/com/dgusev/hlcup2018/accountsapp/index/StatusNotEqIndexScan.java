package com.dgusev.hlcup2018.accountsapp.index;

public class StatusNotEqIndexScan extends AbstractIndexScan {

    private int[] statusIndex1;
    private int[] statusIndex2;
    private int index1;
    private int index2;

    public StatusNotEqIndexScan(IndexHolder indexHolder, String status) {
        super(indexHolder);
        if (status.equals("свободны")) {
            this.statusIndex1 = indexHolder.statusIndex.get(1);
            this.statusIndex2 = indexHolder.statusIndex.get(2);
        } else if (status.equals("всё сложно")) {
            this.statusIndex1 = indexHolder.statusIndex.get(0);
            this.statusIndex2 = indexHolder.statusIndex.get(2);
        } else {
            this.statusIndex1 = indexHolder.statusIndex.get(0);
            this.statusIndex2 = indexHolder.statusIndex.get(1);
        }

    }

    @Override
    public int getNext() {
        if (index1 < statusIndex1.length) {
            if (index2 < statusIndex2.length) {
                int first= statusIndex1[index1];
                int second = statusIndex2[index2];
                if (first > second) {
                    index1++;
                    return first;
                } else {
                    index2++;
                    return second;
                }
            } else {
                return statusIndex1[index1++];
            }
        } else if (index2 < statusIndex2.length) {
            return statusIndex2[index2++];
        } else {
            return -1;
        }
    }
}
