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
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import sun.misc.Unsafe;

import java.util.*;
import java.util.function.Predicate;

@Service
public class AccountService {

    private static final Unsafe UNSAFE = com.dgusev.hlcup2018.accountsapp.service.Unsafe.UNSAFE;

    public  static final int MAX_ID = 1520000;
    private static final Set<String> ALLOWED_SEX = new HashSet<>(Arrays.asList("m", "f"));
    private static final Set<String> ALLOWED_STATUS = new HashSet<>(Arrays.asList("свободны", "всё сложно","заняты"));
    private static final TIntObjectMap<TLongList> phase2 = new TIntObjectHashMap<>(180000, 1);

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

    private static final ThreadLocal<TIntIntMap> groupsCountMapPool = new ThreadLocal<TIntIntMap>() {
        @Override
        protected TIntIntMap initialValue() {
            return new TIntIntHashMap();
        }
    };

    private static final ThreadLocal<Group[]> groupsPool = new ThreadLocal<Group[]>() {
        @Override
        protected Group[] initialValue() {
            Group[] groups =  new Group[10000];
            for (int i =0; i < 10000; i++) {
                groups[i] = new Group();
            }
            return groups;
        }
    };

    private static final ThreadLocal<Group[]> groupsSortArrayPool = new ThreadLocal<Group[]>() {
        @Override
        protected Group[] initialValue() {
            return new Group[50];
        }
    };

    public List<Group> group(List<String> keys, boolean sex, byte status, byte country, int city, int birthYear, byte interes, int like, int joinedYear, int order, int limit, byte keysMask, byte predicatesMask) {
        if (limit <= 0) {
            throw BadRequest.INSTANCE;
        }
        TIntIntMap groupsCountMap = groupsCountMapPool.get();
        groupsCountMap.clear();
        //LikesContainsPredicate
        if (predicatesMask == 64) {
            long address = indexHolder.likesIndex[like];
            if (address != 0) {
                byte size = UNSAFE.getByte(address);
                address++;
                for (int i = 0; i < size; i++) {
                    int id = UNSAFE.getInt(address);
                    address+=4;
                    processRecord2(accountIdMap[id], groupsCountMap, keysMask);
                }
            }
        }
        //JoinedYearPredicate,LikesContainsPredicate
        else if (predicatesMask == (byte)192) {
            long address = indexHolder.likesIndex[like];
            if (address != 0) {
                byte size = UNSAFE.getByte(address);
                address++;
                for (int i = 0; i < size; i++) {
                    int id = UNSAFE.getInt(address);
                    address+=4;
                    Account account = accountIdMap[id];
                    if (JoinedYearPredicate.calculateYear(account.joined) == joinedYear) {
                        processRecord2(account, groupsCountMap, keysMask);
                    }
                }
            }
        }
        //BirthYearPredicate,LikesContainsPredicate
        else if (predicatesMask == 80) {
            long address = indexHolder.likesIndex[like];
            if (address != 0) {
                byte size = UNSAFE.getByte(address);
                address++;
                for (int i = 0; i < size; i++) {
                    int id = UNSAFE.getInt(address);
                    address+=4;
                    Account account = accountIdMap[id];
                    if (BirthYearPredicate.calculateYear(account.birth) == birthYear) {
                        processRecord2(account, groupsCountMap, keysMask);
                    }
                }
            }
        }
        //CityEqPredicate
        else if (predicatesMask == 8) {
            iterateCity(city, groupsCountMap, keysMask);
        }
        //BirthYearPredicate,CityEqPredicate
        else if (predicatesMask == 24) {
            iterateCityBirth(city, birthYear, groupsCountMap, keysMask);
        }
        //CityEqPredicate,JoinedYearPredicate
        else if (predicatesMask == (byte)136) {
            iterateCityJoined(city, joinedYear, groupsCountMap, keysMask);
        }
        //InterestsContainsPredicate
        else if (predicatesMask == 32) {
            iterateInteres(interes, groupsCountMap, keysMask);
        }
        //BirthYearPredicate,InterestsContainsPredicate
        else if (predicatesMask == 48) {
            iterateInteresBirth(interes, birthYear, groupsCountMap, keysMask);
        }
        //CityEqPredicate,JoinedYearPredicate
        else if (predicatesMask == (byte)160) {
            iterateInteresJoined(interes, joinedYear, groupsCountMap, keysMask);
        }
        //CountryEqPredicate
        else if (predicatesMask == 4) {
            iterateCountry(country, groupsCountMap, keysMask);
        }
        //BirthYearPredicate,CountryEqPredicate
        else if (predicatesMask == 20) {
            iterateCountryBirth(country, birthYear, groupsCountMap, keysMask);
        }
        //CountryEqPredicate,JoinedYearPredicate
        else if (predicatesMask == (byte)132) {
            iterateCountryJoined(country, joinedYear, groupsCountMap, keysMask);
        }
        //BirthYearPredicate
        else if (predicatesMask == 16) {
            iterateBirth(birthYear, groupsCountMap, keysMask);
        }
        //BirthYearPredicate,SexEqPredicate
        else if (predicatesMask == 17) {
            iterateBirthSex(birthYear, sex, groupsCountMap, keysMask);
        }
        //BirthYearPredicate,StatusEqPredicate
        else if (predicatesMask == 18) {
            iterateBirthStatus(birthYear, status, groupsCountMap, keysMask);
        }
        //Full scan
        else if (predicatesMask == 0)  {
            return iterateFullScan(keysMask, limit, order);
        }
        //SexEqPredicate
        else if (predicatesMask == 1) {
            return iterateSex(sex, keysMask, limit, order);
        }
        //StatusEqPredicate
        else if (predicatesMask == 2) {
            return iterateStatus(status, keysMask, limit, order);
        }
        //JoinedYearPredicate
        else if (predicatesMask == (byte)128) {
            return iterateJoinedYear(joinedYear, keysMask, limit, order);
        }
        //JoinedYearPredicate,StatusEqPredicate
        else if (predicatesMask == (byte)130) {
            iterateJoinedStatus(joinedYear, status, groupsCountMap, keysMask);
        }
        //JoinedYearPredicate,SexEqPredicate
        else if (predicatesMask == (byte)129) {
            iterateJoinedSex(joinedYear, sex, groupsCountMap, keysMask);
        }
        else {
            return Collections.EMPTY_LIST;
        }
        Group[] groups = groupsSortArrayPool.get();
        Arrays.fill(groups, null);
        Group[] pool = groupsPool.get();
        int counter = 0;
        Group lastGroup = null;
        for (int groupDefinition : groupsCountMap.keys()) {
            int count = groupsCountMap.get(groupDefinition);
            int insertPosition = -1;
            if (lastGroup != null) {
                int cmp = compare(count, groupDefinition, lastGroup.count, lastGroup.values, keys, order);
                if (cmp > 0) {
                    continue;
                }
            }
            for (int i = 0; i < limit; i++) {
                Group group = groups[i];
                if (group == null) {
                    insertPosition = i;
                    break;
                } else {
                    int cmp = compare(count, groupDefinition, group.count, group.values, keys, order);
                    if (cmp > 0) {
                        continue;
                    } else if (cmp < 0) {
                        insertPosition = i;
                        break;
                    }
                }
            }
            if (insertPosition != -1) {
                Group group = pool[counter++];
                group.count = count;
                group.values = groupDefinition;
                System.arraycopy(groups, insertPosition, groups, insertPosition + 1, limit - insertPosition - 1);
                groups[insertPosition] = group;
                if (groups[limit - 1] != null) {
                    lastGroup = groups[limit - 1];
                }
            }
        }
        List<Group> result = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            Group group = groups[i];
            if (group != null) {
                result.add(group);
            } else {
                break;
            }
        }
        return result;
    }

    private void iterateCity(int city, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.cityIndex.get(city);
        for (int id : accounts) {
            Account account = accountIdMap[id];
            processRecord2(account, groupsCountMap, keysMask);
        }
    }

    private void iterateCityBirth(int city,int birthYear, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.cityIndex.get(city);
        for (int id : accounts) {
            Account account = accountIdMap[id];
            if (BirthYearPredicate.calculateYear(account.birth) == birthYear) {
                processRecord2(account, groupsCountMap, keysMask);
            }
        }
    }

    private void iterateCityJoined(int city,int joinedYear, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.cityIndex.get(city);
        for (int id : accounts) {
            Account account = accountIdMap[id];
            if (JoinedYearPredicate.calculateYear(account.joined) == joinedYear) {
                processRecord2(account, groupsCountMap, keysMask);
            }
        }
    }

    private void iterateInteres(byte interes, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.interestsIndex.get(interes);
        for (int id : accounts) {
            Account account = accountIdMap[id & 0x00ffffff];
            processRecord2(account, groupsCountMap, keysMask);
        }
    }

    private void iterateInteresBirth(byte interes,int birthYear, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.interestsIndex.get(interes);
        byte year = (byte)(birthYear - 1900);
        byte[] birthYearIndex = indexHolder.birthYear;
        for (int id : accounts) {
            Account account = accountIdMap[id & 0x00ffffff];
            if (birthYearIndex[account.id] == year) {
                processRecord2(account, groupsCountMap, keysMask);
            }
        }
    }

    private void iterateInteresJoined(byte interes,int joinedYear, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.interestsIndex.get(interes);
        byte year = (byte)(joinedYear - 2000);
        byte[] joinedYearIndex = indexHolder.joinedYear;
        for (int id : accounts) {
            Account account = accountIdMap[id & 0x00ffffff];
            if (joinedYearIndex[account.id] == year) {
                processRecord2(account, groupsCountMap, keysMask);
            }
        }
    }

    private void iterateCountry(byte country, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.countryIndex.get(country);
        for (int id : accounts) {
            Account account = accountIdMap[id];
            processRecord2(account, groupsCountMap, keysMask);
        }
    }

    private void iterateCountryBirth(byte country,int birthYear, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.countryIndex.get(country);
        byte year = (byte)(birthYear - 1900);
        byte[] birthYearIndex = indexHolder.birthYear;
        for (int id : accounts) {
            Account account = accountIdMap[id];
            if (birthYearIndex[account.id] == year) {
                processRecord2(account, groupsCountMap, keysMask);
            }
        }
    }

    private void iterateCountryJoined(byte country,int joinedYear, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.countryIndex.get(country);
        byte year = (byte)(joinedYear - 2000);
        byte[] joinedYearIndex = indexHolder.joinedYear;
        for (int id : accounts) {
            Account account = accountIdMap[id];
            if (joinedYearIndex[account.id] == year) {
                processRecord2(account, groupsCountMap, keysMask);
            }
        }
    }


    private void iterateBirth(int birthYear, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.birthYearIndex.get(birthYear);
        if (accounts != null) {
            for (int id : accounts) {
                Account account = accountIdMap[id];
                processRecord2(account, groupsCountMap, keysMask);
            }
        }
    }

    private void iterateBirthSex(int birthYear, boolean sex, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.birthYearIndex.get(birthYear);
        if (accounts != null) {
            for (int id : accounts) {
                Account account = accountIdMap[id];
                if (account.sex == sex) {
                    processRecord2(account, groupsCountMap, keysMask);
                }
            }
        }
    }

    private void iterateBirthStatus(int birthYear,byte status, TIntIntMap groupsCountMap, byte keysMask) {
        int[] accounts = indexHolder.birthYearIndex.get(birthYear);
        if (accounts != null) {
            for (int id : accounts) {
                Account account = accountIdMap[id];
                if (account.status == status) {
                    processRecord2(account, groupsCountMap, keysMask);
                }
            }
        }
    }

    private void iterateJoinedStatus(int joinedYear, byte status, TIntIntMap groupsCountMap, byte keysMask) {
        if (indexHolder.joinedIndex.get(joinedYear) != null) {
            int[] index = indexHolder.joinedIndex.get(joinedYear);
            for (int id : index) {
                Account account = accountIdMap[id];
                if (account.status == status) {
                    processRecord2(account, groupsCountMap, keysMask);
                }
            }
        }
    }

    private void iterateJoinedSex(int joinedYear, boolean sex, TIntIntMap groupsCountMap, byte keysMask) {
        if (indexHolder.joinedIndex.get(joinedYear) != null) {
            int[] index = indexHolder.joinedIndex.get(joinedYear);
            for (int id : index) {
                Account account = accountIdMap[id];
                if (account.sex == sex) {
                    processRecord2(account, groupsCountMap, keysMask);
                }
            }
        }
    }

    private List<Group> iterateFullScan(byte keysMask, int limit, int order) {
        return iterateIndex(keysMask, limit, order, indexHolder.groups);
    }


    private List<Group> iterateSex(boolean sex, byte keysMask, int limit, int order) {
        long[][] index = sex ? indexHolder.sexGroups[1] : indexHolder.sexGroups[0];
        return iterateIndex(keysMask, limit, order, index);
    }


    private List<Group> iterateStatus(byte status, byte keysMask, int limit, int order) {
        long[][] index = null;
        if (status == 0) {
            index = indexHolder.statusGroups[0];
        } else if (status == 1) {
            index = indexHolder.statusGroups[1];
        } else {
            index = indexHolder.statusGroups[2];
        }
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateJoinedYear(int joinedYear, byte keysMask, int limit, int order) {
        if (joinedYear < 2011 || joinedYear > 2017) {
            return Collections.EMPTY_LIST;
        }
        long[][] index = indexHolder.joinedGroups[joinedYear - 2011];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateIndex(byte keysMask, int limit, int order, long[][] index) {
        long[] groups = null;
        if (keysMask == 0b00000100) {
            groups = index[0];
        } else if (keysMask == 0b00001001) {
            groups = index[1];
        } else if (keysMask == 0b00001000) {
            groups = index[2];
        } else if (keysMask == 0b00000010) {
            groups = index[3];
        } else if (keysMask == 0b00010001) {
            groups = index[4];
        } else if (keysMask == 0b00000001) {
            groups = index[5];
        } else if (keysMask == 0b00010000) {
            groups = index[6];
        } else if (keysMask == 0b00001010) {
            groups = index[7];
        } else if (keysMask == 0b00010010) {
            groups = index[8];
        }
        List<Group> result = new ArrayList<>();
        Group[] groupsArray = groupsPool.get();
        int counter = 0;
        if (order ==  1) {
            for (int i = 0; i < groups.length && i < limit; i++) {
                long grp = groups[i];
                Group group = groupsArray[counter++];
                group.count = (int)grp;
                group.values = (int)(grp >> 32);
                result.add(group);
            }
        } else {
            for (int i = groups.length - 1; i >= 0 && counter < limit; i--) {
                long grp = groups[i];
                Group group = groupsArray[counter++];
                group.count = (int)grp;
                group.values = (int)(grp >> 32);
                result.add(group);
            }
        }
        return result;
    }


    private int compare(int count1, int group1, int count2, int group2, List<String> keys, int order) {
        if (order == 1) {
            int cc = Integer.compare(count1, count2);
            if (cc == 0) {
                return compareGroups(group1, group2, keys);
            } else {
                return cc;
            }
        } else {
            int cc = Integer.compare(count2, count1);
            if (cc == 0) {
                return compareGroups(group2, group1, keys);
            } else {
                return cc;
            }
        }
    }

    private void processRecord2(Account account, TIntIntMap groupsCountMap, byte keysMask) {
        int group = 0;
        if ((keysMask & 0b00000001) != 0) {
            if (account.sex) {
                group|=1;
            }
        }
        if ((keysMask & 0b00000010) != 0) {
            group|=account.status << 1;
        }
        if ((keysMask & 0b00001000) != 0) {
            group|=account.country << 3;
        }
        if ((keysMask & 0b00010000) != 0) {
            group|=account.city << 10;
        }
        if ((keysMask & 0b00000100) != 0) {
            if (account.interests != null && account.interests.length != 0) {
                for (byte interes: account.interests) {
                    int newgroup = group;
                    newgroup|=interes << 20;
                    groupsCountMap.adjustOrPutValue(newgroup, 1, 1);
                }
            }
        } else {
            groupsCountMap.adjustOrPutValue(group, 1, 1);
        }
    }


    private int compareGroups(int group1, int group2, List<String> keys) {
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            if (key.equals("sex")) {
                String g1 = ConvertorUtills.convertSex((group1 & 0b00000001) == 1);
                String g2 = ConvertorUtills.convertSex((group2 & 0b00000001) == 1);
                int cc = g1.compareTo(g2);
                if (cc != 0) {
                    return cc;
                }
            } else if (key.equals("status")) {
                String g1 = ConvertorUtills.convertStatusNumber((byte)((group1 >> 1) & 0b00000011));
                String g2 = ConvertorUtills.convertStatusNumber((byte)((group2 >> 1) & 0b00000011));
                int cc = g1.compareTo(g2);
                if (cc != 0) {
                    return cc;
                }
            } else if (key.equals("interests")) {
                String g1 = dictionary.getInteres((byte)((group1 >> 20) & 0b01111111));
                String g2 = dictionary.getInteres((byte)((group2 >> 20) & 0b01111111));
                if (g1 != null && g2 != null) {
                    int cc = g1.compareTo(g2);
                    if (cc != 0) {
                        return cc;
                    }
                } else if (g1 == null && g2 != null) {
                    return -1;
                } else if (g2 == null && g1 != null) {
                    return 1;
                }
            } else if (key.equals("country")) {
                String g1 = dictionary.getCountry((byte)((group1 >> 3) & 0b01111111));
                String g2 = dictionary.getCountry((byte)((group2 >> 3) & 0b01111111));
                if (g1 != null && g2 != null) {
                    int cc = g1.compareTo(g2);
                    if (cc != 0) {
                        return cc;
                    }
                } else if (g1 == null && g2 != null) {
                    return -1;
                } else if (g2 == null && g1 != null) {
                    return 1;
                }
            } else if (key.equals("city")) {
                String g1 = dictionary.getCity((int)((group1 >> 10) & 0b0000001111111111));
                String g2 = dictionary.getCity((int)((group2 >> 10) & 0b0000001111111111));
                if (g1 != null && g2 != null) {
                    int cc = g1.compareTo(g2);
                    if (cc != 0) {
                        return cc;
                    }
                } else if (g1 == null && g2 != null) {
                    return -1;
                } else if (g2 == null && g1 != null) {
                    return 1;
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
                long address = indexHolder.likesIndex[lid];
                if (address != 0) {
                    int size = UNSAFE.getByte(address);
                    address++;
                    for (int i = 0; i < size; i++) {
                        int l = UNSAFE.getInt(address);
                        address+=4;
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
                    addUserToLikeIndex(account.id, id);
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
                addUserToLikeIndex(likeRequest.liker, likeRequest.likee);
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

    private void addUserToLikeIndex(int liker, int likee) {
        long address = indexHolder.likesIndex[likee];
        if (address == 0) {
            address = UNSAFE.allocateMemory(5);
            UNSAFE.putByte(address, (byte)1);
            UNSAFE.putInt(address + 1, liker);
            indexHolder.likesIndex[likee] = address;
        } else {
            int size = UNSAFE.getByte(address);
            int insertId = size;
            boolean dontInsert = false;
            long pointer = address + 1;
            for (int i = 0; i < size; i++) {
                int id = UNSAFE.getInt(pointer);
                pointer+=4;
                if (id < liker) {
                    insertId = i;
                    break;
                } else if (id == liker) {
                    dontInsert = true;
                    break;
                }
            }
            if (!dontInsert) {
                long newAddress = UNSAFE.allocateMemory(1 + (size+1)*4);
                UNSAFE.putByte(newAddress, (byte)(size + 1));
                if (insertId == 0) {
                    UNSAFE.copyMemory(address + 1, newAddress + 1 + 4, size * 4);
                    UNSAFE.putInt(newAddress + 1, liker);
                } else if (insertId == size) {
                    UNSAFE.copyMemory(address + 1, newAddress + 1, size * 4);
                    UNSAFE.putInt(newAddress + 1 + 4*size, liker);
                } else {
                    UNSAFE.copyMemory(address + 1, newAddress + 1, insertId * 4);
                    UNSAFE.copyMemory(address + 1 + insertId * 4, newAddress + 1 + (insertId+1) * 4, (size - insertId) * 4);
                    UNSAFE.putInt(newAddress + 1 + 4*insertId, liker);
                }
                indexHolder.likesIndex[likee] = newAddress;
                UNSAFE.freeMemory(address);
            }
        }
    }

    public Account findById(int id) {
        return accountIdMap[id];
    }

    public void finishLoad() {
        try {
            indexHolder.init(this.accountList, size, true);
            indexHolder.resetTempListArray();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    private List<IndexScan> getAvailableIndexScan(List<Predicate<Account>> predicates) {
        List<IndexScan> indexScans = ObjectPool.acquireIndexScanList();
        Iterator<Predicate<Account>> predicateIterator = predicates.iterator();
        AbstractPredicate usefulIndex = null;
        for (int i = 0; i < predicates.size(); i++) {
            Predicate<Account> predicate =  predicates.get(i);
            if (predicate instanceof AbstractPredicate) {
                AbstractPredicate abstractPredicate = (AbstractPredicate) predicate;
                if (abstractPredicate.getIndexCordiality() != Integer.MAX_VALUE) {
                    if (usefulIndex == null) {
                        usefulIndex = abstractPredicate;
                    } else {
                        if (abstractPredicate.getIndexCordiality() < usefulIndex.getIndexCordiality()) {
                            usefulIndex = abstractPredicate;
                        }
                    }
                }
            }
        }
        if (usefulIndex != null) {
            indexScans.add(usefulIndex.createIndexScan(indexHolder));
            predicates.remove(usefulIndex);
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
                System.out.println(LAST_UPDATE_TIMESTAMP);
                new Thread(() -> {
                        System.out.println("Start update indexes " + new Date());
                System.out.println("Start update likes" + new Date());
                long t1 = System.currentTimeMillis();
                phase2.forEachEntry((i, tLongList) -> {
                    Account account = accountIdMap[i];
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
                System.out.println("Finish update likes" + new Date() + " took " + (t2 - t1));
                }).start();
                indexHolder.init(accountList, size, false);
                //System.gc();
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
