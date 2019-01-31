package com.dgusev.hlcup2018.accountsapp.pool;

import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.LikeRequest;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TIntHashSet;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ObjectPool {


    private static ThreadLocal<ArrayDeque<ByteBuffer>> buffersPool = new ThreadLocal<ArrayDeque<ByteBuffer>>() {
        @Override
        protected ArrayDeque<ByteBuffer> initialValue() {
            ArrayDeque<ByteBuffer> byteBuffers =  new ArrayDeque<>(100);
            for (int i = 0; i < 100; i++) {
                byteBuffers.addLast(ByteBuffer.allocateDirect(10000));
            }
            return byteBuffers;
        }
    };

    private static ThreadLocal<AccountService.Score[]> scorePool = new ThreadLocal<AccountService.Score[]>() {
        @Override
        protected AccountService.Score[] initialValue() {
            AccountService.Score[] array =   new AccountService.Score[20000];
            for (int i = 0; i < 20000; i++) {
                array[i] =new AccountService.Score();
            }
            return array;
        }
    };

    private static ArrayDeque<LikeRequest> likeRequestPool = new ArrayDeque<LikeRequest>();
    private static ArrayDeque<TLongArrayList> likeListPool = new ArrayDeque<TLongArrayList>();
    static {
            for (int i = 0; i < 720000; i++) {
                likeRequestPool.add(new LikeRequest());
            }
        for (int i = 0; i < 180000; i++) {
            likeListPool.add(new TLongArrayList(4));
        }
    };

    private static ThreadLocal<ArrayDeque<List<Account>>> suggestListPool = new ThreadLocal<ArrayDeque<List<Account>>>() {
        @Override
        protected ArrayDeque<List<Account>> initialValue() {
            return new ArrayDeque<>(20);
        }
    };

    private static ThreadLocal<List<Account>> filterListPool = new ThreadLocal<List<Account>>() {
        @Override
        protected List<Account> initialValue() {
            return new ArrayList<>(100);
        }
    };

    public static ByteBuffer acquireBuffer() {
        ArrayDeque<ByteBuffer> local = buffersPool.get();
        ByteBuffer buffer = local.pollFirst();
        if (buffer != null) {
            buffer.clear();
        } else {
            buffer = ByteBuffer.allocateDirect(10000);
            buffer.clear();
        }
        return buffer;
    }

    public static void releaseBuffer(ByteBuffer byteBuffer) {
        buffersPool.get().addLast(byteBuffer);
    }

    public static AccountService.Score[] acquireScore() {
        return scorePool.get();
    }
    public static LikeRequest acquireLikeRequest() {
        LikeRequest likeRequest = likeRequestPool.pollFirst();
        if (likeRequest != null) {
            return likeRequest;
        } else {
            return new LikeRequest();
        }
    }

    public static TLongArrayList acquireLikeList() {
        TLongArrayList tLongArrayList = likeListPool.pollFirst();
        if (tLongArrayList != null) {
            return tLongArrayList;
        } else {
            return new TLongArrayList();
        }
    }

    public static List<Account> acquireSuggestList() {
        ArrayDeque<List<Account>> local = suggestListPool.get();
        List<Account> list = local.pollFirst();
        if (list != null) {
            list.clear();
        } else {
            list =  new ArrayList<>(5);
        }
        return list;
    }

    public static void releaseSuggestList(List<Account> group) {
        suggestListPool.get().addLast(group);
    }

    public static List<Account> acquireFilterList() {
        List<Account> list =  filterListPool.get();
        list.clear();
        return list;
    }

    public static void init() {}



}
