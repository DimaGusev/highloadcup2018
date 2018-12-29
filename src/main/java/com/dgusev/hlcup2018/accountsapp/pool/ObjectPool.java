package com.dgusev.hlcup2018.accountsapp.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ObjectPool {

    private static final ConcurrentLinkedDeque<List<String>> groupsPools = new ConcurrentLinkedDeque<>();

    static {
        for (int i = 0; i < 100000; i++) {
            List<String> list  = new ArrayList<>(5);
            list.clear();
            groupsPools.addLast(list);
        }
    }


    public static List<String> acquireGroup() {
        List<String> group = groupsPools.pollFirst();
        if (group != null) {
            group.clear();
        } else {
            List<String> list = new ArrayList<>(5);
            groupsPools.addLast(list);
            group = list;
        }
        return group;
    }

    public static void releaseGroup(List<String> group) {
        groupsPools.addLast(group);
    }


}
