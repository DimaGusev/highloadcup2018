package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.index.*;
import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.*;
import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.predicate.*;

import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;

@Service
public class AccountService {
    public  static final int MAX_ID = 1520000;
    private static final Set<String> ALLOWED_SEX = new HashSet<>(Arrays.asList("m", "f"));
    private static final Set<String> ALLOWED_STATUS = new HashSet<>(Arrays.asList("свободны", "всё сложно","заняты"));
    private static final Pattern EMAIL_PATTERN = Pattern.compile("[0-9a-zA-z]+@[0-9a-zA-z]+\\\\.[0-9a-zA-z]+");
    private static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger(0);
    private static final TIntObjectMap<TLongList> phase2 = new TIntObjectHashMap<>(180000, 1);
    private static final TIntObjectMap<TIntList> additionalLikes = new TIntObjectHashMap<>();

    private static final Comparator<Similarity> SIMILARITY_COMPARATOR = Comparator.comparingDouble((Similarity s) -> s.similarity).reversed();

    private AtomicInteger r1 = new AtomicInteger(0);
    private AtomicInteger r2 = new AtomicInteger(0);
    private AtomicInteger r3 = new AtomicInteger(0);
    private AtomicInteger r4 = new AtomicInteger(0);
    private AtomicInteger r5 = new AtomicInteger(0);
    private AtomicInteger r6 = new AtomicInteger(0);
    private AtomicInteger s1 = new AtomicInteger(0);
    private AtomicInteger s2 = new AtomicInteger(0);
    private AtomicInteger s3 = new AtomicInteger(0);
    private AtomicInteger s4 = new AtomicInteger(0);
    private AtomicInteger s5 = new AtomicInteger(0);
    private AtomicInteger s6 = new AtomicInteger(0);


    @Autowired
    private NowProvider nowProvider;

    @Autowired
    private Dictionary dictionary;

    @Autowired
    private IndexHolder indexHolder;

    @Autowired
    private AccountConverter accountConverter;

    private Account[] accountList = new Account[MAX_ID];
    private volatile int size;
    private Account[] accountIdMap = new Account[MAX_ID];

    private Set<String> emails = new HashSet<>();
    private Set<String> phones = new HashSet<>();

    public static volatile long LAST_UPDATE_TIMESTAMP;

    public List<Account> filter(List<Predicate<Account>> predicates, int limit) {
        if (limit <= 0) {
            throw new BadRequest();
        }
        List<IndexScan> indexScans = getAvailableIndexScan(predicates);
        if (!indexScans.isEmpty()) {
            Predicate<Account> accountPredicate = andPredicates(predicates);
            List<Account> result = ObjectPool.acquireFilterList();
            int count = 0;
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }

                Account account = accountIdMap[next];
                if (accountPredicate.test(account)) {
                    result.add(account);
                    count++;
                    if (count == limit) {
                        break;
                    }
                }
            }
            return result;
        } else {
          // if (true) {
           //     return Collections.EMPTY_LIST;
          // }
            Predicate<Account> accountPredicate = andPredicates(predicates);
            return filterSeqScan(accountPredicate, limit);
        }
    }

    private List<Account> filterSeqScan(Predicate<Account> predicate, int limit) {

        List<Account> result = ObjectPool.acquireFilterList();;
        int count = 0;
        if (limit == 0) {
            return new ArrayList<>();
        }
        for (int i = 0; i< size; i++) {
            Account account = accountList[i];
            if (predicate.test(account)) {
                result.add(account);
                count++;
                if (count == limit) {
                    break;
                }
            }
        }
        return result;
    }

    public List<Group> group(List<String> keys, List<Predicate<Account>> predicates, int order, int limit) {
        if (limit <= 0) {
            throw new BadRequest();
        }
        if (true) {
            return Collections.EMPTY_LIST;
        }
        List<IndexScan> indexScans = getAvailableIndexScan(predicates);
        TLongObjectMap<IntegerHolder> groupHashMap = new TLongObjectHashMap<>();
        TLongObjectMap<List<String>> groupNameMap = new TLongObjectHashMap<>();
        List<String> tmpGroupList = new ArrayList<>();
        if (!indexScans.isEmpty()) {
            Predicate<Account> accountPredicate = andPredicates(predicates);
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }
                Account account = accountIdMap[next];
                if (accountPredicate.test(account)) {
                    processRecord(account, groupHashMap, groupNameMap , keys, tmpGroupList);
                }
            }
        } else {
            Predicate<Account> accountPredicate = andPredicates(predicates);
            for (int i = 0; i < size; i++) {
                Account account = accountList[i];
                if (accountPredicate.test(account)) {
                    processRecord(account, groupHashMap, groupNameMap, keys, tmpGroupList);
                }
            }
        }
        Group[] groups = new Group[limit];
        Group lastGroup = null;
        for (long hash : groupHashMap.keys()) {
            int count = groupHashMap.get(hash).count;
            int insertPosition = -1;
            if (lastGroup != null) {
                int cmp = compare(count, groupNameMap.get(hash), lastGroup.count, lastGroup.values, order);
                if (cmp > 0) {
                    ObjectPool.releaseGroup(groupNameMap.get(hash));
                    continue;
                }
            }
            for (int i = 0; i < groups.length; i++) {
                Group group = groups[i];
                if (group == null) {
                    insertPosition = i;
                    break;
                } else {
                    int cmp = compare(count, groupNameMap.get(hash), group.count, group.values, order);
                    if (cmp > 0) {
                        continue;
                    } else if (cmp < 0) {
                        insertPosition = i;
                        break;
                    }
                }
            }
            if (insertPosition != -1) {
                Group group = new Group();
                group.count = groupHashMap.get(hash).count;
                group.values = new ArrayList<>(groupNameMap.get(hash));
                System.arraycopy(groups, insertPosition, groups, insertPosition + 1, limit - insertPosition - 1);
                groups[insertPosition] = group;
                if (groups[groups.length - 1] != null) {
                    lastGroup = groups[groups.length - 1];
                }
            }
            ObjectPool.releaseGroup(groupNameMap.get(hash));
        }
        List<Group> result = new ArrayList<>();
        for (int i = 0; i < groups.length; i++) {
            Group group = groups[i];
            if (group != null) {
                result.add(group);
            } else {
                break;
            }
        }
        return result;
    }

    private int compare(int count1, List<String> group1, int count2, List<String> group2, int order) {
        if (order == 1) {
            int cc = Integer.compare(count1, count2);
            if (cc == 0) {
                return compareGroups(group1, group2);
            } else {
                return cc;
            }
        } else {
            int cc = Integer.compare(count2, count1);
            if (cc == 0) {
                return compareGroups(group2, group1);
            } else {
                return cc;
            }
        }
    }

    private void processRecord(Account account, TLongObjectMap<IntegerHolder> groupHashMap, TLongObjectMap<List<String>> groupNameMap, List<String> keys, List<String> group) {
        group.clear();
        long hashcode =  0;
        int ksize = keys.size();
        for (int i = 0; i < ksize; i++) {
            String key = keys.get(i);
            if (key.equals("sex")) {
                group.add(ConvertorUtills.convertSex(account.sex));
                hashcode = 31* hashcode + (Boolean.valueOf(account.sex).hashCode());
            } else if (key.equals("status")) {
                String status = ConvertorUtills.convertStatusNumber(account.status);
                group.add(status);
                hashcode = 31* hashcode + (status.hashCode());
            } else if (key.equals("interests")) {

            } else if (key.equals("country")) {
                String country = dictionary.getCountry(account.country);
                group.add(country);
                hashcode = 31* hashcode + (country == null ? 0 : country.hashCode());
            } else if (key.equals("city")) {
                String city = dictionary.getCity(account.city);
                group.add(city);
                hashcode = 31* hashcode + (city == null ? 0 : city.hashCode());
            } else {
                throw new BadRequest();
            }
        }
        if (keys.contains("interests")) {
            if (account.interests != null && account.interests.length != 0) {
                for (byte inter : account.interests) {
                    String interes = dictionary.getInteres(inter);
                    long newHashcode = hashcode;
                    newHashcode = 31 * newHashcode + interes.hashCode();
                    List<String> newGroup = ObjectPool.acquireGroup();
                    int size = group.size();
                    for (int i = 0; i < size; i++) {
                        newGroup.add(group.get(i));
                    }
                    newGroup.add(interes);
                    incrementGroup(groupHashMap, groupNameMap, newGroup, newHashcode);
                    ObjectPool.releaseGroup(newGroup);
                }
            }
        } else {
            incrementGroup(groupHashMap, groupNameMap, group, hashcode);
        }
    }

    private void incrementGroup(TLongObjectMap<IntegerHolder> groupHashMap, TLongObjectMap<List<String>> groupNameMap, List<String> group, long hash) {
        if (!groupNameMap.containsKey(hash)) {
            List<String> grp = ObjectPool.acquireGroup();
            for (int i = 0; i < group.size(); i++) {
                grp.add(group.get(i));
            }
            groupNameMap.put(hash, grp);
        }
        IntegerHolder integerHolder = groupHashMap.get(hash);
        if (integerHolder == null) {
            integerHolder = new IntegerHolder();
            groupHashMap.put(hash, integerHolder);
        }
        integerHolder.count++;
    }

    private int compareGroups(List<String> g1, List<String> g2) {
        for (int i = 0; i < g1.size(); i++) {
            if (g1.get(i) == null && g2.get(i) != null) {
                return -1;
            } else if (g2.get(i) == null && g1.get(i) != null) {
                return 1;
            } else if (g2.get(i) != null && g1.get(i) != null) {
                int cc = g1.get(i).compareTo(g2.get(i));
                if (cc != 0) {
                    return cc;
                }
            }
        }
        return 0;
    }

    private static final ThreadLocal<boolean[]> recommendSet = new ThreadLocal<boolean[]>() {
        @Override
        protected boolean[] initialValue() {
            return new boolean[1500000];
        }
    };

    private static final ThreadLocal<List<Account>> recommendResult = new ThreadLocal<List<Account>>() {
        @Override
        protected List<Account> initialValue() {
            return new ArrayList<>(20);
        }
    };

    private static final ThreadLocal<Score[]> recommendSortArray = new ThreadLocal<Score[]>() {
        @Override
        protected Score[] initialValue() {
            return new Score[20];
        }
    };

    public List<Account> recommend(int id, byte country, int city, int limit) {
        if (limit <= 0) {
            throw new BadRequest();
        }

        Account account = accountIdMap[id];
        if (account == null) {
            throw new NotFoundRequest();
        }
        if (account.interests == null || account.interests.length == 0) {
            return Collections.EMPTY_LIST;
        }
        if (country == 0 || city == 0) {
            return Collections.EMPTY_LIST;
        }
        boolean[] recommend = recommendSet.get();
        resetArray(recommend);
        int sex = account.sex ? 1 : 0;
        List<Account> result1 = ObjectPool.acquireRecommendList();
        List<Account> result2 = ObjectPool.acquireRecommendList();
        List<Account> result3 = ObjectPool.acquireRecommendList();
        List<Account> result4 = ObjectPool.acquireRecommendList();
        List<Account> result5 = ObjectPool.acquireRecommendList();
        List<Account> result6 = ObjectPool.acquireRecommendList();
        fetchRecommendations2(id, sex, account, country, city, limit, recommend, result1, result2, result3, result4, result5, result6);
        try {
            List<Score> result = ObjectPool.acquireRecommendListResult();
            fillList(account, result, limit, result1, result2, result3, result4, result5, result6);
            AccountService.Score[] sortArray = recommendSortArray.get();
            int lastIndex = 0;
            int resultSize = result.size();
            for (int i = 0; i < resultSize; i++) {
                Score item = result.get(i);
                if (lastIndex == limit) {
                    Score lastScore = sortArray[lastIndex - 1];
                    if (lastScore.score > item.score) {
                        continue;
                    } else if (lastScore.score == item.score) {
                        if (lastScore.account.id < item.account.id) {
                            continue;
                        }
                    }
                    int insertIndex = 0;
                    for (int j = 0; j < limit; j++) {
                        if (item.score > sortArray[j].score) {
                            insertIndex = j;
                            break;
                        } else if (item.score == sortArray[j].score) {
                            if (item.account.id < sortArray[j].account.id) {
                                insertIndex = j;
                                break;
                            }
                        }
                    }
                    System.arraycopy(sortArray, insertIndex, sortArray, insertIndex + 1, limit - insertIndex - 1);
                    sortArray[insertIndex] = item;
                } else  {
                    int insertIndex = -1;
                    for (int j = 0; j < lastIndex; j++) {
                        if (item.score > sortArray[j].score) {
                            insertIndex = j;
                            break;
                        } else if (item.score == sortArray[j].score) {
                            if (item.account.id < sortArray[j].account.id) {
                                insertIndex = j;
                                break;
                            }
                        }
                    }
                    if (insertIndex == -1) {
                        sortArray[lastIndex] = item;
                    } else {
                        System.arraycopy(sortArray, insertIndex, sortArray, insertIndex + 1, limit - insertIndex - 1);
                        sortArray[insertIndex] = item;
                    }
                    lastIndex++;
                }
            }

            List<Account> response = recommendResult.get();
            response.clear();
            for (int i = 0; i < lastIndex; i++) {
                response.add(sortArray[i].account);
            }
            return response;
        } finally {
            ObjectPool.releaseRecommendList(result1);
            ObjectPool.releaseRecommendList(result2);
            ObjectPool.releaseRecommendList(result3);
            ObjectPool.releaseRecommendList(result4);
            ObjectPool.releaseRecommendList(result5);
            ObjectPool.releaseRecommendList(result6);
        }
    }

    private void fetchRecommendations(int id, int sex, Account account, byte country, int city, int limit, boolean[] recommend, List<Account> result1, List<Account> result2, List<Account> result3, List<Account> result4, List<Account> result5, List<Account> result6) {
        int counter = 0;
        int result1Size = 0;
        for (byte interes : account.interests) {
            for (int aId : indexHolder.interestsIndex.get(interes)) {
                counter++;

                if (aId >>> 31 == sex) {
                    continue;
                }
                aId = aId & 0x00ffffff;
                if (aId != id) {
                    if (counter != 1 && recommend[aId]) {
                        continue;
                    }
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    if (acc.premium) {
                        if (acc.status == 0) {
                            result1.add(acc);
                            result1Size++;
                        } else if (acc.status == 1) {
                            if (result1Size < limit) {
                                result2.add(acc);
                            }
                        } else {
                            if (result1Size < limit) {
                                result3.add(acc);
                            }
                        }
                    } else {
                        if (acc.status == 0) {
                            if (result1Size < limit) {
                                result4.add(acc);
                            }
                        } else if (acc.status == 1) {
                            if (result1Size < limit) {
                                result5.add(acc);
                            }
                        } else {
                            if (result1Size < limit) {
                                result6.add(acc);
                            }
                        }
                    }
                    recommend[aId] = true;
                }
            }
        }
    }


    private void fetchRecommendations2(int id, int sex, Account account, byte country, int city, int limit, boolean[] recommend, List<Account> result1, List<Account> result2, List<Account> result3, List<Account> result4, List<Account> result5, List<Account> result6) {

        TByteObjectMap<int[]> prio1;
        TByteObjectMap<int[]> prio2;
        TByteObjectMap<int[]> prio3;
        TByteObjectMap<int[]> prio4;
        TByteObjectMap<int[]> prio5;
        TByteObjectMap<int[]> prio6;

        if (sex == 1) {
            prio1 = indexHolder.sexFalsePremiumState0Index;
            prio2 = indexHolder.sexFalsePremiumState1Index;
            prio3 = indexHolder.sexFalsePremiumState2Index;
            prio4 = indexHolder.sexFalseNonPremiumState0Index;
            prio5 = indexHolder.sexFalseNonPremiumState1Index;
            prio6 = indexHolder.sexFalseNonPremiumState2Index;
        } else {
            prio1 = indexHolder.sexTruePremiumState0Index;
            prio2 = indexHolder.sexTruePremiumState1Index;
            prio3 = indexHolder.sexTruePremiumState2Index;
            prio4 = indexHolder.sexTrueNonPremiumState0Index;
            prio5 = indexHolder.sexTrueNonPremiumState1Index;
            prio6 = indexHolder.sexTrueNonPremiumState2Index;
        }

        int totalCount = 0;

        for (byte interes : account.interests) {
            for (int aId : prio1.get(interes)) {
                if (aId != id) {
                    if (recommend[aId]) {
                        continue;
                    }
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    result1.add(acc);
                    totalCount++;
                    recommend[aId] = true;
                }
            }
        }
        if (totalCount >= limit) {
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio2.get(interes)) {
                if (aId != id) {
                    if (recommend[aId]) {
                        continue;
                    }
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    result2.add(acc);
                    totalCount++;
                    recommend[aId] = true;
                }
            }
        }
        if (totalCount >= limit) {
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio3.get(interes)) {
                if (aId != id) {
                    if (recommend[aId]) {
                        continue;
                    }
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    result3.add(acc);
                    totalCount++;
                    recommend[aId] = true;
                }
            }
        }
        if (totalCount >= limit) {
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio4.get(interes)) {
                if (aId != id) {
                    if (recommend[aId]) {
                        continue;
                    }
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    result4.add(acc);
                    totalCount++;
                    recommend[aId] = true;
                }
            }
        }
        if (totalCount >= limit) {
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio5.get(interes)) {
                if (aId != id) {
                    if (recommend[aId]) {
                        continue;
                    }
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    result5.add(acc);
                    totalCount++;
                    recommend[aId] = true;
                }
            }
        }
        if (totalCount >= limit) {
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio6.get(interes)) {
                if (aId != id) {
                    if (recommend[aId]) {
                        continue;
                    }
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    result6.add(acc);
                    totalCount++;
                    recommend[aId] = true;
                }
            }
        }
    }

    private void resetArray(boolean[] arr) {
        Arrays.fill(arr, false);
    }

    private static void fillList(Account account, List<Score> result, int limit, List<Account> result1, List<Account> result2, List<Account> result3, List<Account> result4, List<Account> result5, List<Account> result6) {
        Score[] pool = ObjectPool.acquireScore();
        int counter = 0;
        int res1Cnt = result1.size();
        for (int i = 0; i < res1Cnt; i++) {
            Score score = pool[counter++];
            calculateScore(score, 16000, account, result1.get(i));
            result.add(score);
        }
        if (result.size() < limit) {
            int cnt = result2.size();
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 14000, account, result2.get(i));
                result.add(score);
            }
        }
        if (result.size() < limit) {
            int cnt = result3.size();
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 12000, account, result3.get(i));
                result.add(score);
            }
        }
        if (result.size() < limit) {
            int cnt = result4.size();
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 6000, account, result4.get(i));
                result.add(score);
            }
        }
        if (result.size() < limit) {
            int cnt = result5.size();
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 4000, account, result5.get(i));
                result.add(score);
            }
        }
        if (result.size() < limit) {
            int cnt = result6.size();
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 2000, account, result6.get(i));
                result.add(score);
            }
        }
    }

    private static int interestsMatched(byte[] myInterests, byte[] othersInterests) {
        int index1 = 0;
        int index2 = 0;
        int count = 0;
        while (index1 < myInterests.length && index2 < othersInterests.length) {
            byte int1 = myInterests[index1];
            byte int2 = othersInterests[index2];
            if (int1 == int2) {
                count++;
                index1++;
                index2++;
            } else {
                if (int1 < int2) {
                    index1++;
                } else {
                    index2++;
                }
            }
        }
        return count;
    }

    private static boolean contains(byte[] arrray, byte element) {
        for (int i = 0; i < arrray.length; i++) {
            if (arrray[i] == element) {
                return true;
            }
        }
        return false;
    }


    public List<Account> suggest(int id, List<Predicate<Account>> predicates, int limit) {
        Account account = accountIdMap[id];
        if (account == null) {
            throw new NotFoundRequest();
        }
        if (account.likes == null || account.likes.length == 0) {
            return Collections.EMPTY_LIST;
        }
        predicates.add(new SexEqPredicate(account.sex));
        predicates.add(a -> a.id != id);

        boolean targetSex = !account.sex;

        TIntHashSet myLikes = ObjectPool.acquireTIntHash();
        TIntHashSet suggests = ObjectPool.acquireTIntHash();
        for (long like: account.likes) {
            int lid = (int)(like >> 32);
            boolean newLike = myLikes.add(lid);
            if (newLike) {
                int[] likers = indexHolder.likesIndex[lid];
                if (likers != null) {
                    for (int l : likers) {
                        suggests.add(l);
                    }
                }
            }
        }
        Similarity[] suggestResult = null;
        int size = 0;
        List<Account> result = null;
        if (suggests.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        try {
            int[] likersIndex = ObjectPool.acquireLikersArray();
            if (suggests.size() > likersIndex.length) {
                System.out.println("Suggest array not enough " + suggests.size());
            }
            suggests.toArray(likersIndex);
            Arrays.sort(likersIndex, 0, suggests.size());
            reverse(likersIndex, 0, suggests.size());
            IndexScan likersIndexScan = new ArrayIndexScan(likersIndex, 0, suggests.size());

            List<IndexScan> indexScans = getAvailableIndexScan(predicates);
            indexScans.add(likersIndexScan);
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            Predicate<Account> accountPredicate = andPredicates(predicates);
            suggestResult = ObjectPool.acquireSimilarityList();
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }
                Account acc = accountIdMap[next];
                if (accountPredicate.test(acc)) {
                    Similarity similarity = ObjectPool.acquireSimilarity();
                    similarity.account = acc;
                    similarity.similarity = getSimilarity(account, acc);
                    suggestResult[size++] = similarity;
                }
            }
            result = ObjectPool.acquireSuggestList();
            TIntSet likersSet = new TIntHashSet();
            double maxSimilarity = Double.MAX_VALUE;
            while (true) {
                Similarity s = findNextMaxSimilarity(suggestResult, size, maxSimilarity);
                if (s ==  null) {
                    break;
                }
                maxSimilarity = s.similarity;
                for (int j = 0; j < s.account.likes.length; j++) {
                    int lid = (int)(s.account.likes[j] >> 32);
                    if (!myLikes.contains(lid) && accountIdMap[lid].sex == targetSex) {
                        if (!likersSet.contains(lid)) {
                            likersSet.add(lid);
                            result.add(accountIdMap[lid]);
                            if (result.size() == limit) {
                                break;
                            }
                        }
                    }
                }
                if (result.size() == limit) {
                    break;
                }
            }

            return result;
        } finally {
            ObjectPool.releaseTIntHash(myLikes);
            ObjectPool.releaseTIntHash(suggests);
            if (suggestResult != null) {
                try {
                    for (int i = 0; i < size; i++) {
                        ObjectPool.releaseSimilarity(suggestResult[i]);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private Similarity findNextMaxSimilarity(Similarity[] array, int size, double oldSimilarity) {
        Similarity max = null;
        double currentMax = 0;
        for (int i =0; i < size; i++) {
            Similarity similarity = array[i];
            if (similarity.similarity < oldSimilarity) {
                if (similarity.similarity > currentMax) {
                    currentMax = similarity.similarity;
                    max = similarity;
                }
            }
        }
        return max;
    }

    private void reverse(int[] array, int from, int to) {
        int size = (to - from);
        int half = from + size / 2;
        for (int i = from; i < half; i++) {
            int tmp = array[i];
            array[i] = array[to - 1 - i];
            array[to - 1 - i] = tmp;
        }
    }

    private void reverse(long[] array) {
        int size = array.length;
        int half = size / 2;
        for (int i = 0; i < half; i++) {
            long tmp = array[i];
            array[i] = array[size - 1 - i];
            array[size - 1 - i] = tmp;
        }
    }

    private double getSimilarity(Account a1, Account a2) {
        int index1 = 0;
        int index2 = 0;
        double similarity = 0;
        while (index1 < a1.likes.length && index2 < a2.likes.length) {
            int like1 = (int)(a1.likes[index1] >> 32);
            int like2 = (int)(a2.likes[index2] >> 32);
            if (like1 == like2) {
                double sum1 = (int)a1.likes[index1];
                double sum2 = (int)a2.likes[index2];
                int cnt1 = 1;
                int cnt2 = 1;
                index1++;
                index2++;
                while (index1 < a1.likes.length && (int)(a1.likes[index1] >> 32) == like1) {
                    sum1+=(int)a1.likes[index1];
                    cnt1++;
                    index1++;
                }
                while (index2 < a2.likes.length && (int)(a2.likes[index2] >> 32) == like2) {
                    sum2+=(int)a2.likes[index2];
                    cnt2++;
                    index2++;
                }
                double t1 = sum1/cnt1;
                double t2 = sum2/cnt2;
                if (t1 == t2) {
                    similarity+=1;
                } else {
                    similarity += 1 / Math.abs(t1 - t2);
                }

            } else {
                if (like1 < like2) {
                    index2++;
                } else {
                    index1++;
                }
            }
        }
        return similarity;
    }

    public synchronized void load(Account account) {
        if (size == 0) {
            accountList[size] = account;
            size++;
        } else {
            for (int i = 0; i < size; i++) {
                if (account.id > accountList[i].id) {
                    System.arraycopy(accountList, i, accountList, i+1, size - i);
                    accountList[i] = account;
                    size++;
                    break;
                }
            }
        }
        accountIdMap[account.id] =  account;
        emails.add(account.email);
        if (account.phone != null) {
            phones.add(account.phone);
        }
    }

    public synchronized void loadSequentially(Account account) {
        accountList[size] = account;
        size++;
        accountIdMap[account.id] =  account;
        emails.add(account.email);
        if (account.phone != null) {
            phones.add(account.phone);
        }
    }

    public synchronized void rearrange() {
        for (int i = 0; i < size/2; i++) {
            Account tmp = accountList[i];
            accountList[i] = accountList[size - 1 - i];
            accountList[size - 1 - i] = tmp;
        }
        Arrays.sort(accountList, 0, size, (a1,a2) -> {
            return Integer.compare(a2.id, a1.id);
        });
    }


    public synchronized void add(AccountDTO accountDTO) {
        if (accountDTO.id == -1 || accountDTO.email == null || accountDTO.sex == null || accountDTO.birth == Integer.MIN_VALUE || accountDTO.joined == Integer.MIN_VALUE || accountDTO.status == null) {
            throw new BadRequest();
        }
        if (!ALLOWED_SEX.contains(accountDTO.sex)) {
            throw new BadRequest();
        }
        if (!ALLOWED_STATUS.contains(accountDTO.status)) {
            throw new BadRequest();
        }
        if (!accountDTO.email.contains("@")) {
            throw new BadRequest();
        }
        if (accountIdMap[accountDTO.id] != null) {
            throw new BadRequest();
        }
        if (emails.contains(accountDTO.email)) {
            throw new BadRequest();
        }
        if (accountDTO.phone != null && phones.contains(accountDTO.phone)) {
            throw new BadRequest();
        }
        Account account = accountConverter.convert(accountDTO);
        this.load(account);
        if (account.likes != null && account.likes.length != 0) {
            int prev = -1;
            for (int j = 0; j < account.likes.length; j++) {
                int id = (int)(account.likes[j] >> 32);
                if (prev != id) {
                    TIntList tIntList = additionalLikes.get(id);
                    if (tIntList == null) {
                        tIntList = new TIntArrayList();
                        additionalLikes.put(id, tIntList);
                    }
                    if (!tIntList.contains(account.id)) {
                        tIntList.add(account.id);
                    }
                }
                prev = id;
            }
        }
        if (LAST_UPDATE_TIMESTAMP == 0) {
            synchronized (this) {
                if (LAST_UPDATE_TIMESTAMP == 0) {
                    LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
                    new Thread(new IndexUpdater()).start();
                }
            }
        }
        LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
    }

    public synchronized void update(AccountDTO accountDTO) {
        if (accountDTO.sex != null && !ALLOWED_SEX.contains(accountDTO.sex)) {
            throw new BadRequest();
        }
        if (accountDTO.status != null && !ALLOWED_STATUS.contains(accountDTO.status)) {
            throw new BadRequest();
        }
        if (accountDTO.id >= MAX_ID) {
            throw new NotFoundRequest();
        }
        Account oldAcc = accountIdMap[accountDTO.id];
        if (oldAcc == null) {
            throw new NotFoundRequest();
        }
        if (accountDTO.email != null && !accountDTO.email.contains("@")) {
            throw new BadRequest();
        }
        if (accountDTO.status != null) {
            ConvertorUtills.convertStatusNumber(accountDTO.status);
        }
        if (accountDTO.email != null && !oldAcc.email.equals(accountDTO.email)) {
            if (emails.contains(accountDTO.email)) {
                throw new BadRequest();
            }
        }
        if ((accountDTO.phone != null && oldAcc.phone == null) ||( accountDTO.phone != null && !oldAcc.phone.equals(accountDTO.phone))) {
            if (phones.contains(accountDTO.phone)) {
                throw new BadRequest();
            }
        }

        if (accountDTO.email != null && !oldAcc.email.equals(accountDTO.email)) {
            emails.remove(oldAcc.email);
            emails.add(accountDTO.email);
            oldAcc.email = accountDTO.email;
        }
        if (accountDTO.fname != null) {
            oldAcc.fname = dictionary.getOrCreateFname(accountDTO.fname);
        }
        if (accountDTO.sname != null) {
            oldAcc.sname = dictionary.getOrCreateSname(accountDTO.sname);
        }
        if (accountDTO.phone != null && oldAcc.phone != null && !oldAcc.phone.equals(accountDTO.phone)) {
            phones.remove(oldAcc.phone);
            phones.add(accountDTO.phone);
            oldAcc.phone = accountDTO.phone;
        } else if (oldAcc.phone == null && accountDTO.phone != null) {
            phones.add(accountDTO.phone);
            oldAcc.phone = accountDTO.phone;
        }
        if (accountDTO.sex != null) {
            oldAcc.sex = ConvertorUtills.convertSex(accountDTO.sex);
        }
        if (accountDTO.birth != Integer.MIN_VALUE) {
            oldAcc.birth = accountDTO.birth;
        }
        if (accountDTO.country != null) {
            oldAcc.country = dictionary.getOrCreateCountry(accountDTO.country);
        }
        if (accountDTO.city != null) {
            oldAcc.city = dictionary.getOrCreateCity(accountDTO.city);
        }
        if (accountDTO.joined != Integer.MIN_VALUE) {
            oldAcc.joined = accountDTO.joined;
        }
        if (accountDTO.status != null) {
            oldAcc.status = ConvertorUtills.convertStatusNumber(accountDTO.status);
        }
        if (accountDTO.interests != null) {
            byte[] values = new byte[accountDTO.interests.length];
            for (int i = 0; i < accountDTO.interests.length; i ++) {
                values[i] = dictionary.getInteres(accountDTO.interests[i]);
            }
            oldAcc.interests = values;
            Arrays.sort(oldAcc.interests);
        }
        if (accountDTO.premiumStart != 0) {
            oldAcc.premiumStart = accountDTO.premiumStart;
            oldAcc.premiumFinish = accountDTO.premiumFinish;
            oldAcc.premium = oldAcc.premiumStart != 0 && oldAcc.premiumStart <= nowProvider.getNow() && (oldAcc.premiumFinish == 0 || oldAcc.premiumFinish > nowProvider.getNow());

        }
        if (accountDTO.likes != null) {
            System.out.println("Update likes!");
            //oldAcc.likes = accountDTO.likes;
        }
        if (LAST_UPDATE_TIMESTAMP == 0) {
            synchronized (this) {
                if (LAST_UPDATE_TIMESTAMP == 0) {
                    LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
                    new Thread(new IndexUpdater()).start();
                }
            }
        }
        LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
    }


    public synchronized void like(List<LikeRequest> likeRequests) {
            for (int i = 0; i < likeRequests.size(); i++) {
                LikeRequest likeRequest = likeRequests.get(i);
                if (likeRequest.likee >= MAX_ID || likeRequest.liker >= MAX_ID) {
                    throw BadRequest.INSTANCE;
                }
                if (likeRequest.likee == -1 || likeRequest.liker == -1 || likeRequest.ts == -1 || accountIdMap[likeRequest.likee] == null || accountIdMap[likeRequest.liker] == null) {
                    throw BadRequest.INSTANCE;
                }
            }
            for (int i = 0; i < likeRequests.size(); i++) {
                LikeRequest likeRequest = likeRequests.get(i);
                Account account = accountIdMap[likeRequest.liker];
                long like = 0;
                like = (long) likeRequest.likee << 32;
                like = (likeRequest.ts | like);
                if (!phase2.containsKey(likeRequest.liker)) {
                    phase2.put(likeRequest.liker, ObjectPool.acquireLikeList());
                }
                phase2.get(likeRequest.liker).add(like);
                /*long[] oldArray = account.likes;
                if (oldArray == null) {
                    account.likes = new long[1];
                    account.likes[0] = like;
                } else {
                    account.likes = new long[oldArray.length + 1];
                    System.arraycopy(oldArray, 0, account.likes, 0, oldArray.length);
                    account.likes[oldArray.length] = like;
                }
                Arrays.sort(account.likes);
                reverse(account.likes);*/
            }
            if (LAST_UPDATE_TIMESTAMP == 0) {
                synchronized (this) {
                    if (LAST_UPDATE_TIMESTAMP == 0) {
                        LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
                        new Thread(new IndexUpdater()).start();
                    }
                }
            }
            LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
    }

    public Account findById(int id) {
        return accountIdMap[id];
    }

    public void finishLoad() {
        try {
            indexHolder.init(this.accountList, size, null);
            indexHolder.resetTempListArray();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    private List<IndexScan> getAvailableIndexScan(List<Predicate<Account>> predicates) {
        List<IndexScan> indexScans = ObjectPool.acquireIndexScanList();
        Iterator<Predicate<Account>> iterator = predicates.iterator();
        while (iterator.hasNext()) {
            Predicate<Account> predicate = iterator.next();
            if (predicate instanceof CountryEqPredicate) {
                CountryEqPredicate countryEqPredicate = (CountryEqPredicate) predicate;
                indexScans.add(new CountryEqIndexScan(indexHolder, countryEqPredicate.getCounty()));
                iterator.remove();
            } else if (predicate instanceof CountryNullPredicate) {
                CountryNullPredicate countryNullPredicate = (CountryNullPredicate) predicate;
                if (countryNullPredicate.getNill() == 1) {
                    indexScans.add(new CountryNullIndexScan(indexHolder));
                    iterator.remove();
                }
            } else if (predicate instanceof StatusEqPredicate) {
                StatusEqPredicate statusEqPredicate = (StatusEqPredicate) predicate;
                indexScans.add(new StatusEqIndexScan(indexHolder, statusEqPredicate.getStatus()));
                iterator.remove();
            } else if (predicate instanceof InterestsContainsPredicate) {
                InterestsContainsPredicate interestsContainsPredicate = (InterestsContainsPredicate) predicate;
                indexScans.add(new InterestsContainsIndexScan(indexHolder, interestsContainsPredicate.getInterests()));
                iterator.remove();
            } else if (predicate instanceof InterestsAnyPredicate) {
                InterestsAnyPredicate interestsAnyPredicate = (InterestsAnyPredicate) predicate;
                indexScans.add(new InterestsAnyIndexScan(indexHolder, interestsAnyPredicate.getInterests()));
                iterator.remove();
            } /*else if (predicate instanceof SexEqPredicate) {
                SexEqPredicate sexEqPredicate = (SexEqPredicate) predicate;
                indexScans.add(new SexEqIndexScan(indexHolder, sexEqPredicate.getSex()));
                iterator.remove();
            } else if (predicate instanceof StatusNEqPredicate) {
                StatusNEqPredicate statusNEqPredicate = (StatusNEqPredicate) predicate;
                indexScans.add(new StatusNotEqIndexScan(indexHolder, statusNEqPredicate.getStatus()));
                iterator.remove();
            } */else if (predicate instanceof CityNullPredicate) {
                CityNullPredicate cityNullPredicate = (CityNullPredicate) predicate;
                if (cityNullPredicate.getNill() == 1) {
                    indexScans.add(new CityNullIndexScan(indexHolder));
                    iterator.remove();
                }
            } else if (predicate instanceof CityEqPredicate) {
                CityEqPredicate cityEqPredicate = (CityEqPredicate) predicate;
                indexScans.add(new CityEqIndexScan(indexHolder, cityEqPredicate.getCity()));
                iterator.remove();
            } else if (predicate instanceof CityAnyPredicate) {
                CityAnyPredicate cityAnyPredicate = (CityAnyPredicate) predicate;
                indexScans.add(new CityAnyIndexScan(indexHolder, cityAnyPredicate.getCities()));
                iterator.remove();
            } else if (predicate instanceof BirthYearPredicate) {
                BirthYearPredicate birthYearPredicate = (BirthYearPredicate) predicate;
                indexScans.add(new BirthYearIndexScan(indexHolder, birthYearPredicate.getYear()));
                iterator.remove();
            } else if (predicate instanceof PremiumNowPredicate) {
                indexScans.add(new PremiumIndexScan(indexHolder));
                iterator.remove();
            } else if (predicate instanceof EmailDomainPredicate) {
                EmailDomainPredicate emailDomainPredicate = (EmailDomainPredicate) predicate;
                indexScans.add(new EmailDomainIndexScan(indexHolder, emailDomainPredicate.getDomain()));
                iterator.remove();
            } else if (predicate instanceof JoinedYearPredicate) {
                JoinedYearPredicate joinedYearPredicate = (JoinedYearPredicate) predicate;
                indexScans.add(new JoinedYearIndexScan(indexHolder, joinedYearPredicate.getYear()));
                iterator.remove();
            } else if (predicate instanceof LikesContainsPredicate) {
                LikesContainsPredicate likesContainsPredicate = (LikesContainsPredicate) predicate;
                indexScans.add(new LikesContainsIndexScan(indexHolder, likesContainsPredicate.getLikes()));
                iterator.remove();
            } else if (predicate instanceof FnameEqPredicate) {
                FnameEqPredicate fnameEqPredicate = (FnameEqPredicate) predicate;
                indexScans.add(new FnameEqIndexScan(indexHolder, fnameEqPredicate.getFname()));
                iterator.remove();
            } else if (predicate instanceof FnameAnyPredicate) {
                FnameAnyPredicate fnameAnyPredicate = (FnameAnyPredicate) predicate;
                indexScans.add(new FnameAnyIndexScan(indexHolder, fnameAnyPredicate.getFnames()));
                iterator.remove();
            } else if (predicate instanceof FnameNullPredicate) {
                FnameNullPredicate fnameNullPredicate = (FnameNullPredicate) predicate;
                if (fnameNullPredicate.getNill() == 1) {
                    indexScans.add(new FnameNullIndexScan(indexHolder));
                    iterator.remove();
                }
            } else if (predicate instanceof SnameEqPredicate) {
                SnameEqPredicate snameEqPredicate = (SnameEqPredicate) predicate;
                indexScans.add(new SnameEqIndexScan(indexHolder, snameEqPredicate.getSname()));
                iterator.remove();
            } else if (predicate instanceof SnameNullPredicate) {
                SnameNullPredicate snameNullPredicate = (SnameNullPredicate) predicate;
                if (snameNullPredicate.getNill() == 1) {
                    indexScans.add(new SnameNullIndexScan(indexHolder));
                    iterator.remove();
                }
            } else if (predicate instanceof EmailEqPredicate) {
                EmailEqPredicate emailEqPredicate = (EmailEqPredicate) predicate;
                indexScans.add(new EmailEqIndexScan(indexHolder, emailEqPredicate.getEmail()));
                iterator.remove();
            } else if (predicate instanceof PhoneCodePredicate) {
                PhoneCodePredicate phoneCodePredicate = (PhoneCodePredicate) predicate;
                indexScans.add(new PhoneCodeIndexScan(indexHolder, phoneCodePredicate.getCode()));
                iterator.remove();
            } else if (predicate instanceof PhoneEqPredicate) {
                PhoneEqPredicate phoneEqPredicate = (PhoneEqPredicate) predicate;
                indexScans.add(new PhoneEqIndexScan(indexHolder, phoneEqPredicate.getPhone()));
                iterator.remove();
            } else if (predicate instanceof PhoneNullPredicate) {
                PhoneNullPredicate phoneNullPredicate = (PhoneNullPredicate) predicate;
                if (phoneNullPredicate.getNill() == 0) {
                    indexScans.add(new PhoneNotNullIndexScan(indexHolder));
                    iterator.remove();
                }
            } else if (predicate instanceof PremiumNullPredicate) {
                PremiumNullPredicate premiumNullPredicate = (PremiumNullPredicate) predicate;
                if (premiumNullPredicate.getNill() == 0) {
                    indexScans.add(new PremiumNotNullIndexScan(indexHolder));
                    iterator.remove();
                }
            }
        }

        return indexScans;
    }

    private Predicate<Account> andPredicates(List<Predicate<Account>> predicates) {
        Predicate<Account> accountPredicate = null;
        if (predicates.isEmpty()) {
            accountPredicate = foo -> true;
        } else {
            accountPredicate = predicates.get(0);
            for (int i = 1; i < predicates.size(); i++) {
                accountPredicate = accountPredicate.and(predicates.get(i));
            }
        }
        return accountPredicate;
    }

    private class IndexUpdater implements Runnable {

        @Override
        public void run() {
            while (LAST_UPDATE_TIMESTAMP == 0 || System.currentTimeMillis() - LAST_UPDATE_TIMESTAMP  < 1000){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            try {
                System.out.println("Start update indexes " + new Date());
                System.out.println("Start update likes" + new Date());
                long t1 = System.currentTimeMillis();
                int[] likeCount = new int[1];
                phase2.forEachEntry((i, tLongList) -> {
                    Account account = accountIdMap[i];
                    for (int j = 0; j < tLongList.size(); j++) {
                        int id = (int)(tLongList.get(j) >> 32);
                        if (!containsLike(account, id)) {
                            TIntList tIntList = additionalLikes.get(id);
                            if (tIntList == null) {
                                tIntList = new TIntArrayList();
                                additionalLikes.put(id, tIntList);
                            }
                            if (!tIntList.contains(i)) {
                                likeCount[0]++;
                                tIntList.add(i);
                            }
                        }
                    }
                    if (account.likes == null || account.likes.length == 0) {
                        account.likes = new long[tLongList.size()];
                        tLongList.toArray(account.likes);
                    } else {
                        int oldCount = account.likes.length;
                        int newSize = oldCount + tLongList.size();
                        long[] newArray = new long[newSize];
                        System.arraycopy(account.likes, 0, newArray, 0, oldCount);
                        for (int j = oldCount; j < newSize; j++) {
                            newArray[j] = tLongList.get(j - oldCount);
                        }
                        account.likes = newArray;
                    }
                    Arrays.sort(account.likes);
                    reverse(account.likes);
                    return true;
                });
                long t2 = System.currentTimeMillis();
                System.out.println("Finish update likes" + new Date() + " took " + (t2-t1));
                System.out.println("Like count=" + likeCount[0] + ", accounts=" + additionalLikes.size());
                indexHolder.init(accountList, size, additionalLikes);
                System.gc();
                System.out.println("End update indexes " + new Date());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private boolean containsLike(Account account, int like) {
        if (account.likes == null || account.likes.length == 0){
            return false;
        }
        for (long l : account.likes) {
            int id = (int)(l >> 32);
            if (id == like) {
                return true;
            }
            if (id < like) {
                return false;
            }
        }
        return false;
    }

    private static class IntegerHolder {
        public int count;
    }

    public static class Similarity {
        public Account account;
        public double similarity;
    }

    private static class Like  {
        public int ts;
        public int id;
    }

    public static class Score {
        public Account account;
        public long score;
    }

    private static void calculateScore(Score score, int base, Account my, Account other) {
        score.account = other;
        long value = base;
        int int1 = interestsMatched(my.interests, other.interests);
        value+=int1;
        int bd = Math.abs(my.birth - other.birth);
        value = (value << 32) | (0x7fffffff - bd);
        score.score = value;
    }

}
