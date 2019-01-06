package com.dgusev.hlcup2018.accountsapp.pool;

import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.LikeRequest;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ObjectPool {

    private static ThreadLocal<ArrayDeque<List<String>>> groupsPools = new ThreadLocal<ArrayDeque<List<String>>>() {
        @Override
        protected ArrayDeque<List<String>> initialValue() {
            return new ArrayDeque<>(500000);
        }
    };

    private static ThreadLocal<ArrayDeque<AccountService.Similarity>> similarityPool = new ThreadLocal<ArrayDeque<AccountService.Similarity>>() {
        @Override
        protected ArrayDeque<AccountService.Similarity> initialValue() {
            ArrayDeque arrayDeque =  new ArrayDeque<>(20000);
            for (int i = 0; i < 20000; i++) {
                arrayDeque.add(new AccountService.Similarity());
            }
            return arrayDeque;
        }
    };

    private static ThreadLocal<ArrayDeque<LikeRequest>> likeRequestPool = new ThreadLocal<ArrayDeque<LikeRequest>>() {
        @Override
        protected ArrayDeque<LikeRequest> initialValue() {
            ArrayDeque<LikeRequest> arrayDeque =  new ArrayDeque<>(200);
            for (int i = 0; i < 200; i++) {
                arrayDeque.add(new LikeRequest());
            }
            return arrayDeque;
        }
    };

    private static ThreadLocal<ArrayDeque<List<AccountService.Similarity>>> listSimilarityPool = new ThreadLocal<ArrayDeque<List<AccountService.Similarity>>>() {
        @Override
        protected ArrayDeque<List<AccountService.Similarity>> initialValue() {
            return new ArrayDeque<>(20);
        }
    };


    private static ThreadLocal<ArrayDeque<List<Account>>> suggestListPool = new ThreadLocal<ArrayDeque<List<Account>>>() {
        @Override
        protected ArrayDeque<List<Account>> initialValue() {
            return new ArrayDeque<>(20);
        }
    };



    private static ThreadLocal<ArrayDeque<List<Account>>> recommendListPools = new ThreadLocal<ArrayDeque<List<Account>>>() {
        @Override
        protected ArrayDeque<List<Account>> initialValue() {
            return new ArrayDeque<>(50);
        }
    };

    private static ThreadLocal<ArrayDeque<TIntHashSet>> intHashSetPools = new ThreadLocal<ArrayDeque<TIntHashSet>>() {
        @Override
        protected ArrayDeque<TIntHashSet> initialValue() {
            return new ArrayDeque<>(20);
        }
    };

    private static ThreadLocal<byte[]> formatterPool = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[10000];
        }
    };

    private static ThreadLocal<List<Account>> filterListPool = new ThreadLocal<List<Account>>() {
        @Override
        protected List<Account> initialValue() {
            return new ArrayList<>(100);
        }
    };

    private static ThreadLocal<List<IndexScan>> indexScanPool = new ThreadLocal<List<IndexScan>>() {
        @Override
        protected List<IndexScan> initialValue() {
            return new ArrayList<>(10);
        }
    };

    private static ThreadLocal<int[]> likersPool = new ThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[30000];
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

    public static List<Account> acquireRecommendList() {
        ArrayDeque<List<Account>> local = recommendListPools.get();
        List<Account> group = local.pollFirst();
        if (group != null) {
            group.clear();
        } else {
            group =  new ArrayList<>(5);
        }
        return group;
    }

    public static void releaseRecommendList(List<Account> group) {
        recommendListPools.get().addLast(group);
    }

    public static TIntHashSet acquireTIntHash() {
        ArrayDeque<TIntHashSet> local = intHashSetPools.get();
        TIntHashSet hashSet = local.pollFirst();
        if (hashSet != null) {
            hashSet.clear();
        } else {
            hashSet =  new TIntHashSet();
        }
        return hashSet;
    }

    public static void releaseTIntHash(TIntHashSet hashSet) {
        intHashSetPools.get().addLast(hashSet);
    }

    public static byte[] acquireFormatterArray() {
        return formatterPool.get();
    }

    public static AccountService.Similarity acquireSimilarity() {
        ArrayDeque<AccountService.Similarity> local = similarityPool.get();
        AccountService.Similarity similarity = local.pollFirst();
        if (similarity != null) {
            return similarity;
        } else {
            return new AccountService.Similarity();
        }
    }

    public static void releaseSimilarity(AccountService.Similarity similarity) {
        similarityPool.get().addLast(similarity);
    }

    public static LikeRequest acquireLikeRequest() {
        ArrayDeque<LikeRequest> local = likeRequestPool.get();
        LikeRequest likeRequest = local.pollFirst();
        if (likeRequest != null) {
            likeRequest.ts =0;
            likeRequest.likee=0;
            likeRequest.liker=0;
            return likeRequest;
        } else {
            return new LikeRequest();
        }
    }

    public static void releaseLikeRequest(LikeRequest likeRequest) {
        likeRequestPool.get().addLast(likeRequest);
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

    public static List<AccountService.Similarity> acquireSimilarityList() {
        ArrayDeque<List<AccountService.Similarity>> local = listSimilarityPool.get();
        List<AccountService.Similarity> list = local.pollFirst();
        if (list != null) {
            list.clear();
        } else {
            list =  new ArrayList<>(5);
        }
        return list;
    }

    public static void releaseSimilarityList(List<AccountService.Similarity> list) {
        listSimilarityPool.get().addLast(list);
    }


    public static int[] acquireLikersArray() {
        return likersPool.get();
    }


    public static List<Account> acquireFilterList() {
        List<Account> list =  filterListPool.get();
        list.clear();
        return list;
    }

    public static List<IndexScan> acquireIndexScanList() {
        List<IndexScan> list =  indexScanPool.get();
        list.clear();
        return list;
    }


}
