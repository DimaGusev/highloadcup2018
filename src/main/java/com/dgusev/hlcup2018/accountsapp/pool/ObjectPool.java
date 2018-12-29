package com.dgusev.hlcup2018.accountsapp.pool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ObjectPool {

    /*private static final ConcurrentLinkedDeque<List<String>> groupsPools = new ConcurrentLinkedDeque<>();

    static {
        for (int i = 0; i < 100000; i++) {
            List<String> list  = new ArrayList<>(5);
            list.clear();
            groupsPools.addLast(list);
        }
    }
    */
    private static ThreadLocal<ArrayDeque<List<String>>> groupsPools = new ThreadLocal<ArrayDeque<List<String>>>() {
        @Override
        protected ArrayDeque<List<String>> initialValue() {
            return new ArrayDeque<>(500000);
        }
    };


    public static List<String> acquireGroup() {
        ArrayDeque<List<String>> local = groupsPools.get();
        List<String> group = local.pollFirst();
        if (group != null) {
            group.clear();
        } else {
            group =  new ArrayList<>(5);
        }
        return group;
    }

    public static void releaseGroup(List<String> group) {
        groupsPools.get().addLast(group);
    }


}
