package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.index.*;
import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.*;
import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.predicate.*;

import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.*;
import gnu.trove.map.hash.*;
import gnu.trove.set.TIntSet;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;
import io.netty.channel.epoll.EpollServer;
import io.netty.channel.epoll.RequestHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import sun.misc.Unsafe;

import javax.annotation.PostConstruct;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

@Service
public class AccountService {

    private static final Unsafe UNSAFE = com.dgusev.hlcup2018.accountsapp.service.Unsafe.UNSAFE;

    public  static final int MAX_ID = 1520000;
    private static final Set<String> ALLOWED_SEX = new HashSet<>(Arrays.asList("m", "f"));
    private static final Set<String> ALLOWED_STATUS = new HashSet<>(Arrays.asList("свободны", "всё сложно","заняты"));
    private static final TIntObjectMap<TLongList> phase2 = new TIntObjectHashMap<>(180000, 1);



    public static CountDownLatch countDownLatch = new CountDownLatch(1);


    @Autowired
    private NowProvider nowProvider;

    @Autowired
    private Dictionary dictionary;

    @Autowired
    private IndexHolder indexHolder;

    @Autowired
    private AccountConverter accountConverter;

    @Autowired
    private EpollServer epollServer;

    private Account[] accountList = new Account[MAX_ID];
    private volatile int size;
    private Account[] accountIdMap = new Account[MAX_ID];

    private TLongSet emails = new TLongHashSet();
    private TLongSet phones = new TLongHashSet();

    public static volatile long LAST_UPDATE_TIMESTAMP;

    private static final ThreadLocal<AbstractPredicate[]> predicateArray = new ThreadLocal<AbstractPredicate[]>() {
        @Override
        protected AbstractPredicate[] initialValue() {
            return new AbstractPredicate[20];
        }
    };

    public List<Account> filter(List<AbstractPredicate> predicates, int limit, int predicateMask) {
        if (limit <= 0) {
            throw new BadRequest();
        }
        if ((predicateMask & 3) == 3) {
            FnameAnyPredicate fnameAnyPredicate = null;
            SexEqPredicate sexEqPredicate = null;
            for (int i = 0; i < predicates.size(); i++) {
                AbstractPredicate predicate = predicates.get(i);
                if (predicate instanceof FnameAnyPredicate) {
                    fnameAnyPredicate = (FnameAnyPredicate) predicate;
                } else if (predicate instanceof SexEqPredicate) {
                    sexEqPredicate = (SexEqPredicate) predicate;
                }
            }
            boolean allNull = true;
            for (int i = 0; i < fnameAnyPredicate.getFnames().length; i++) {
                int fname = fnameAnyPredicate.getFnames()[i];
                byte s = dictionary.getFnameSex(fname);
                if (s == 3) {
                    allNull = false;
                } else {
                    byte targetSex = sexEqPredicate.getSex() ? (byte)2: 1;
                    if (targetSex != s) {
                        fnameAnyPredicate.getFnames()[i] = 0;
                    } else {
                        allNull = false;
                    }
                }
            }
            if (allNull) {
                return Collections.EMPTY_LIST;
            }
        }
        if ((predicateMask & 0b00001100) == 12) {
            BirthYearPredicate birthYearPredicate = null;
            CountryEqPredicate countryEqPredicate = null;

            for (int i = 0; i < predicates.size(); i++) {
                AbstractPredicate predicate = predicates.get(i);
                if (predicate instanceof BirthYearPredicate) {
                    birthYearPredicate = (BirthYearPredicate) predicate;
                } else if (predicate instanceof CountryEqPredicate) {
                    countryEqPredicate = (CountryEqPredicate) predicate;
                }
            }
            predicates.remove(birthYearPredicate);
            predicates.remove(countryEqPredicate);
            int index = 55* countryEqPredicate.getCounty() + (birthYearPredicate.getYear() - 1950);
            int[] indexArray = indexHolder.countryBirthIndex.get(index);
            if (indexArray == null) {
                return Collections.EMPTY_LIST;
            }

            AbstractPredicate[] accountPredicate = predicateArray.get();
            int predicateSize = andPredicates(predicates, accountPredicate);
            List<Account> result = ObjectPool.acquireFilterList();
            int count = 0;
            int indexPosition = 0;
            final int indexSize = indexArray.length;
            while (indexPosition < indexSize) {
                int next = indexArray[indexPosition++];
                if (next == -1) {
                    break;
                }

                Account account = accountIdMap[next];
                boolean test = true;
                for (int i = 0; i < predicateSize; i++) {
                    if (!accountPredicate[i].test(account)) {
                        test = false;
                        break;
                    }
                }
                if (test) {
                    result.add(account);
                    count++;
                    if (count == limit) {
                        break;
                    }
                }
            }
            return result;
        }

        if ((predicateMask & 0b00010000) != 0) {
            if ((predicateMask & 0b00100100) != 0) {

                CountryEqPredicate countryEqPredicate = null;
                CityEqPredicate cityEqPredicate = null;
                InterestsContainsPredicate interestsContainsPredicate = null;
                AbstractPredicate usefulIndex = null;
                for (int i = 0; i < predicates.size(); i++) {
                    AbstractPredicate predicate =  predicates.get(i);
                    if (predicate instanceof AbstractPredicate) {
                        if (predicate instanceof CountryEqPredicate) {
                            countryEqPredicate = (CountryEqPredicate) predicate;
                        }
                        if (predicate instanceof CityEqPredicate) {
                            cityEqPredicate = (CityEqPredicate) predicate;
                        }
                        if (predicate instanceof InterestsContainsPredicate) {
                            interestsContainsPredicate = (InterestsContainsPredicate) predicate;
                        }
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
                if (usefulIndex == countryEqPredicate || usefulIndex == cityEqPredicate) {
                   IndexScan indexScan = interestsContainsPredicate.createIndexScan(indexHolder);
                   predicates.remove(interestsContainsPredicate);
                    AbstractPredicate[] accountPredicate = predicateArray.get();
                    int predicateSize = andPredicates(predicates, accountPredicate);
                    List<Account> result = ObjectPool.acquireFilterList();
                    int count = 0;
                    while (true) {
                        int next = indexScan.getNext();
                        if (next == -1) {
                            break;
                        }
                        Account account = accountIdMap[next];
                        boolean test = true;
                        for (int i = 0; i < predicateSize; i++) {
                            if (!accountPredicate[i].test(account)) {
                                test = false;
                                break;
                            }
                        }
                        if (test) {
                            result.add(account);
                            count++;
                            if (count == limit) {
                                break;
                            }
                        }
                    }
                    return result;
                }

            }
        }
        List<IndexScan> indexScans = getAvailableIndexScan(predicates);
        if (!indexScans.isEmpty()) {
            AbstractPredicate[] accountPredicate = predicateArray.get();
            int predicateSize = andPredicates(predicates, accountPredicate);
            List<Account> result = ObjectPool.acquireFilterList();
            int count = 0;
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }

                Account account = accountIdMap[next];
                boolean test = true;
                for (int i = 0; i < predicateSize; i++) {
                    if (!accountPredicate[i].test(account)) {
                        test = false;
                        break;
                    }
                }
                if (test) {
                    result.add(account);
                    count++;
                    if (count == limit) {
                        break;
                    }
                }
            }
            return result;
        } else {
            AbstractPredicate[] accountPredicate = predicateArray.get();
            int predicateSize = andPredicates(predicates, accountPredicate);
            return filterSeqScan(accountPredicate, predicateSize, limit);
        }
    }

    private List<Account> filterSeqScan(AbstractPredicate[] predicates, int predicateSize, int limit) {

        List<Account> result = ObjectPool.acquireFilterList();;
        int count = 0;
        if (limit == 0) {
            return new ArrayList<>();
        }
        for (int i = 0; i< size; i++) {
            Account account = accountList[i];
            boolean test = true;
            for (int j = 0; j < predicateSize; j++) {
                if (!predicates[j].test(account)) {
                    test = false;
                    break;
                }
            }
            if (test) {
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
                address+=5;
                int prev = -1;
                for (int i = 0; i < size; i++) {
                    int id = UNSAFE.getInt(address);
                    address+=8;
                    if (id != prev) {
                        processRecord2(accountIdMap[id], groupsCountMap, keysMask);
                        prev = id;
                    }
                }
            }
        }
        //JoinedYearPredicate,LikesContainsPredicate
        else if (predicatesMask == (byte)192) {
            long address = indexHolder.likesIndex[like];
            if (address != 0) {
                byte size = UNSAFE.getByte(address);
                address+=5;
                int prev = -1;
                for (int i = 0; i < size; i++) {
                    int id = UNSAFE.getInt(address);
                    address+=8;
                    Account account = accountIdMap[id];
                    if (JoinedYearPredicate.calculateYear(account.joined) == joinedYear) {
                        if (id != prev) {
                            processRecord2(accountIdMap[id], groupsCountMap, keysMask);
                            prev = id;
                        }
                    }
                }
            }
        }
        //BirthYearPredicate,LikesContainsPredicate
        else if (predicatesMask == 80) {
            long address = indexHolder.likesIndex[like];
            if (address != 0) {
                byte size = UNSAFE.getByte(address);
                address+=5;
                int prev = -1;
                for (int i = 0; i < size; i++) {
                    int id = UNSAFE.getInt(address);
                    address+=8;
                    Account account = accountIdMap[id];
                    if (BirthYearPredicate.calculateYear(account.birth) == birthYear) {
                        if (id != prev) {
                            processRecord2(accountIdMap[id], groupsCountMap, keysMask);
                            prev = id;
                        }
                    }
                }
            }
        }
        //CityEqPredicate
        else if (predicatesMask == 8) {
            return iterateCity(city, keysMask, limit, order);
        }
        //BirthYearPredicate,CityEqPredicate
        else if (predicatesMask == 24) {
            return iterateCityBirth(city, birthYear, keysMask, limit, order);
        }
        //CityEqPredicate,JoinedYearPredicate
        else if (predicatesMask == (byte)136) {
            return iterateCityJoined(city, joinedYear, keysMask, limit, order);
        }
        //InterestsContainsPredicate
        else if (predicatesMask == 32) {
            return iterateInteres(interes, keysMask, limit, order);
        }
        //BirthYearPredicate,InterestsContainsPredicate
        else if (predicatesMask == 48) {
            return iterateInteresBirth(interes, birthYear, keysMask, limit, order);
        }
        //InterestsContainsPredicate,JoinedYearPredicate
        else if (predicatesMask == (byte)160) {
            return iterateInteresJoined(interes, joinedYear, keysMask, limit, order);
        }
        //CountryEqPredicate
        else if (predicatesMask == 4) {
            return iterateCountry(country, keysMask, limit, order);
        }
        //BirthYearPredicate,CountryEqPredicate
        else if (predicatesMask == 20) {
            return iterateCountryBirth(country, birthYear, keysMask, limit, order);
        }
        //CountryEqPredicate,JoinedYearPredicate
        else if (predicatesMask == (byte)132) {
            return iterateCountryJoined(country, joinedYear, keysMask, limit, order);
        }
        //BirthYearPredicate
        else if (predicatesMask == 16) {
            return iterateBirth(birthYear, keysMask, limit, order);
        }
        //BirthYearPredicate,SexEqPredicate
        else if (predicatesMask == 17) {
            return iterateBirthSex(birthYear, sex, keysMask, limit, order);
        }
        //BirthYearPredicate,StatusEqPredicate
        else if (predicatesMask == 18) {
            return iterateBirthStatus(birthYear, status, keysMask, limit, order);
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
            return iterateJoinedStatus(joinedYear, status, keysMask, limit, order);
        }
        //JoinedYearPredicate,SexEqPredicate
        else if (predicatesMask == (byte)129) {
            return iterateJoinedSex(joinedYear, sex, keysMask, limit, order);
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

    private List<Group> iterateCity(int city, byte keysMask, int limit, int order) {
        if (city == 0) {
            return Collections.EMPTY_LIST;
        }
        long[] index = indexHolder.cityGroupsIndex[city - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateCityBirth(int city,int birthYear, byte keysMask, int limit, int order) {
        if (city == 0) {
            return Collections.EMPTY_LIST;
        }
        if (birthYear < 1950 || birthYear >= 2005) {
            return Collections.EMPTY_LIST;
        }
        int number = birthYear - 1950;
        long[] index = indexHolder.cityBirthGroupsIndex[number*700 + city - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateCityJoined(int city,int joinedYear, byte keysMask, int limit, int order) {
        if (city == 0) {
            return Collections.EMPTY_LIST;
        }
        if (joinedYear < 2011 || joinedYear > 2017) {
            return Collections.EMPTY_LIST;
        }
        int number = joinedYear - 2011;
        long[] index = indexHolder.cityJoinedGroupsIndex[number*700 + city - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateInteres(byte interes, byte keysMask, int limit, int order) {
        if (interes == 0) {
            return Collections.EMPTY_LIST;
        }
        long[] index = indexHolder.interesGroupsIndex[interes - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateInteresBirth(byte interes,int birthYear, byte keysMask, int limit, int order) {
        if (interes == 0) {
            return Collections.EMPTY_LIST;
        }
        if (birthYear < 1950 || birthYear >= 2005) {
            return Collections.EMPTY_LIST;
        }
        int number = birthYear - 1950;
        long[] index = indexHolder.interesBirthGroupsIndex[number*90 + interes - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateInteresJoined(byte interes,int joinedYear, byte keysMask, int limit, int order) {
        if (interes == 0) {
            return Collections.EMPTY_LIST;
        }
        if (joinedYear < 2011 || joinedYear > 2017) {
            return Collections.EMPTY_LIST;
        }
        int number = joinedYear - 2011;
        long[] index = indexHolder.interesJoinedGroupsIndex[number*90 + interes - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateCountry(byte country, byte keysMask, int limit, int order) {
        if (country == 0) {
            return Collections.EMPTY_LIST;
        }
        long[] index = indexHolder.countryGroupsIndex[country - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateCountryBirth(byte country,int birthYear, byte keysMask, int limit, int order) {
        if (country == 0) {
            return Collections.EMPTY_LIST;
        }
        if (birthYear < 1950 || birthYear >= 2005) {
            return Collections.EMPTY_LIST;
        }
        int number = birthYear - 1950;
        long[] index = indexHolder.countryBirthGroupsIndex[number*70 + country - 1];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateCountryJoined(byte country,int joinedYear, byte keysMask, int limit, int order) {
        if (country == 0) {
            return Collections.EMPTY_LIST;
        }
        if (joinedYear < 2011 || joinedYear > 2017) {
            return Collections.EMPTY_LIST;
        }
        int number = joinedYear - 2011;
        long[] index = indexHolder.countryJoinedGroupsIndex[number*70 + country - 1];
        return iterateIndex(keysMask, limit, order, index);
    }


    private List<Group> iterateBirth(int birthYear, byte keysMask,int limit, int order) {
        if (birthYear < 1950 || birthYear >= 2005) {
            return Collections.EMPTY_LIST;
        }
        int number = birthYear - 1950;
        long[] index = indexHolder.birthGroupsIndex[number];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateBirthSex(int birthYear, boolean sex, byte keysMask, int limit, int order) {
        if (birthYear < 1950 || birthYear >= 2005) {
            return Collections.EMPTY_LIST;
        }
        int number = birthYear - 1950;
        if (sex) {
            number+=55;
        }
        long[] index = indexHolder.birthSexGroupsIndex[number];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateBirthStatus(int birthYear,byte status, byte keysMask, int limit, int order) {
        if (birthYear < 1950 || birthYear >= 2005) {
            return Collections.EMPTY_LIST;
        }
        int number = birthYear - 1950;
        number+=55*status;
        long[] index = indexHolder.birthStatusGroupsIndex[number];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateJoinedStatus(int joinedYear, byte status, byte keysMask, int limit, int order) {
        if (joinedYear < 2011 || joinedYear > 2017) {
            return Collections.EMPTY_LIST;
        }
        int number = joinedYear - 2011;
        number+=status*7;

        long[] index = indexHolder.joinedStatusGroupsIndex[number];
        return iterateIndex(keysMask, limit, order, index);
    }

    private List<Group> iterateJoinedSex(int joinedYear, boolean sex, byte keysMask, int limit, int order) {
        if (joinedYear < 2011 || joinedYear > 2017) {
            return Collections.EMPTY_LIST;
        }
        int number = joinedYear - 2011;
        if (sex) {
            number+=7;
        }

        long[] index = indexHolder.joinedSexGroupsIndex[number];
        return iterateIndex(keysMask, limit, order, index);
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

    private List<Group> iterateIndex(byte keysMask, int limit, int order, long[] indexAddresses) {
        long address = 0;
        if (keysMask == 0b00000100) {
            address = indexAddresses[0];
        } else if (keysMask == 0b00001001) {
            address = indexAddresses[1];
        } else if (keysMask == 0b00001000) {
            address = indexAddresses[2];
        } else if (keysMask == 0b00000010) {
            address = indexAddresses[3];
        } else if (keysMask == 0b00010001) {
            address = indexAddresses[4];
        } else if (keysMask == 0b00000001) {
            address = indexAddresses[5];
        } else if (keysMask == 0b00010000) {
            address = indexAddresses[6];
        } else if (keysMask == 0b00001010) {
            address = indexAddresses[7];
        } else if (keysMask == 0b00010010) {
            address = indexAddresses[8];
        }
        if (address == 0) {
            return Collections.EMPTY_LIST;
        }
        List<Group> result = new ArrayList<>();
        Group[] groupsArray = groupsPool.get();
        int counter = 0;
        int length = UNSAFE.getShort(address);
        if (order ==  1) {
            address+=2;
            for (int i = 0; i < length && i < limit; i++) {
                long grp = UNSAFE.getLong(address);
                address+=8;
                Group group = groupsArray[counter++];
                group.count = (int)grp;
                group.values = (int)(grp >> 32);
                result.add(group);
            }
        } else {
            address+=2;
            address+=(length - 1)*8;
            for (int i = length - 1; i >= 0 && counter < limit; i--) {
                long grp = UNSAFE.getLong(address);
                address-=8;
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


    private static final ThreadLocal<byte[]> recommendSet = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1500000];
        }
    };

    private static final ThreadLocal<double[]> suggestSet = new ThreadLocal<double[]>() {
        @Override
        protected double[] initialValue() {
            return new double[1500000];
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

    private static final ThreadLocal<TIntIntMap> recommendInterestsCountMapPool = new ThreadLocal<TIntIntMap>() {
        @Override
        protected TIntIntMap initialValue() {
            return new TIntIntHashMap();
        }
    };

    private static final ThreadLocal<Account[][]> recommendArrayPool = new ThreadLocal<Account[][]>() {
        @Override
        protected Account[][] initialValue() {
            Account[][] accounts = new Account[6][];
            for (int i = 0; i < 6; i++) {
                accounts[i] = new Account[15000];
            }
            return accounts;
        }
    };

    private static final ThreadLocal<int[]> recommendArraySizePool = new ThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[6];
        }
    };

    private static final ThreadLocal<Score[]> recommendArrayResultPool = new ThreadLocal<Score[]>() {
        @Override
        protected Score[] initialValue() {
            return new Score[15000];
        }
    };





    public List<Account> recommend(int id, byte country, int city, int limit) {
        if (limit <= 0) {
            throw BadRequest.INSTANCE;
        }

        Account account = accountIdMap[id];
        if (account == null) {
            throw NotFoundRequest.INSTANCE;
        }
        if (account.interests == null || account.interests.length == 0) {
            return Collections.EMPTY_LIST;
        }
        if (country == 0 || city == 0) {
            return Collections.EMPTY_LIST;
        }
        byte[] recommend = recommendSet.get();
        int sex = account.sex ? 1 : 0;
        Account[][] recommendArray = recommendArrayPool.get();
        int[] sizeArr = recommendArraySizePool.get();
        for (int i = 0; i < 6; i++) {
            sizeArr[i] = 0;
        }
        if (city == -1 && country == -1) {
            fetchRecommendations2(id, sex, account, country, city, limit, recommend, recommendArray[0], recommendArray[1], recommendArray[2], recommendArray[3], recommendArray[4], recommendArray[5], sizeArr);
        } else {
            fetchRecommendations2CityCountry(account, country, city, limit, recommend, recommendArray[0], recommendArray[1], recommendArray[2], recommendArray[3], recommendArray[4], recommendArray[5], sizeArr);
        }
        Score[] result = recommendArrayResultPool.get();
        int size = fillList(account, result, limit, recommendArray[0], recommendArray[1], recommendArray[2], recommendArray[3], recommendArray[4], recommendArray[5], sizeArr, recommend);
        AccountService.Score[] sortArray = recommendSortArray.get();
        int lastIndex = 0;
        for (int i = 0; i < size; i++) {
            Score item = result[i];
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
            } else {
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
    }

    private void fetchRecommendations2(int id, int sex, Account account, byte country, int city, int limit, byte[] recommend, Account[] result1, Account[] result2, Account[] result3, Account[] result4, Account[] result5, Account[] result6, int[] sizeArr) {

        TByteObjectMap<int[]> prio1;
        TByteObjectMap<int[]> prio2;
        TByteObjectMap<int[]> prio3;
        TByteObjectMap<int[]> prio4;
        TByteObjectMap<int[]> prio5;
        TByteObjectMap<int[]> prio6;

        int result1Size = 0;
        int result2Size = 0;
        int result3Size = 0;
        int result4Size = 0;
        int result5Size = 0;
        int result6Size = 0;

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
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result1[result1Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio2.get(interes)) {
                if (aId != id) {
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result2[result2Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio3.get(interes)) {
                if (aId != id) {
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result3[result3Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            sizeArr[2] = result3Size;
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio4.get(interes)) {
                if (aId != id) {

                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result4[result4Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            sizeArr[2] = result3Size;
            sizeArr[3] = result4Size;
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio5.get(interes)) {
                if (aId != id) {
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result5[result5Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            sizeArr[2] = result3Size;
            sizeArr[3] = result4Size;
            sizeArr[4] = result5Size;
            return;
        }
        for (byte interes : account.interests) {
            for (int aId : prio6.get(interes)) {
                if (aId != id) {
                    Account acc = accountIdMap[aId];
                    if (country != -1 && acc.country != country) {
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        continue;
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result6[result6Size++] = acc;
                    totalCount++;
                }
            }
        }
        sizeArr[0] = result1Size;
        sizeArr[1] = result2Size;
        sizeArr[2] = result3Size;
        sizeArr[3] = result4Size;
        sizeArr[4] = result5Size;
        sizeArr[5] = result6Size;
    }

    private void fetchRecommendations2CityCountry(Account account, byte country, int city, int limit, byte[] recommend, Account[] result1, Account[] result2, Account[] result3, Account[] result4, Account[] result5, Account[] result6, int[] sizeArr) {

        long[][] prio1;
        long[][] prio2;
        long[][] prio3;
        long[][] prio4;
        long[][] prio5;
        long[][] prio6;

        int result1Size = 0;
        int result2Size = 0;
        int result3Size = 0;
        int result4Size = 0;
        int result5Size = 0;
        int result6Size = 0;

        int index = 0;

        if (city != -1) {
            if (account.sex) {
                prio1 = indexHolder.citySexFalsePremiumState0Index;
                prio2 = indexHolder.citySexFalsePremiumState1Index;
                prio3 = indexHolder.citySexFalsePremiumState2Index;
                prio4 = indexHolder.citySexFalseNonPremiumState0Index;
                prio5 = indexHolder.citySexFalseNonPremiumState1Index;
                prio6 = indexHolder.citySexFalseNonPremiumState2Index;
            } else {
                prio1 = indexHolder.citySexTruePremiumState0Index;
                prio2 = indexHolder.citySexTruePremiumState1Index;
                prio3 = indexHolder.citySexTruePremiumState2Index;
                prio4 = indexHolder.citySexTrueNonPremiumState0Index;
                prio5 = indexHolder.citySexTrueNonPremiumState1Index;
                prio6 = indexHolder.citySexTrueNonPremiumState2Index;
            }
            index = city;
        } else {
            if (account.sex) {
                prio1 = indexHolder.countrySexFalsePremiumState0Index;
                prio2 = indexHolder.countrySexFalsePremiumState1Index;
                prio3 = indexHolder.countrySexFalsePremiumState2Index;
                prio4 = indexHolder.countrySexFalseNonPremiumState0Index;
                prio5 = indexHolder.countrySexFalseNonPremiumState1Index;
                prio6 = indexHolder.countrySexFalseNonPremiumState2Index;
            } else {
                prio1 = indexHolder.countrySexTruePremiumState0Index;
                prio2 = indexHolder.countrySexTruePremiumState1Index;
                prio3 = indexHolder.countrySexTruePremiumState2Index;
                prio4 = indexHolder.countrySexTrueNonPremiumState0Index;
                prio5 = indexHolder.countrySexTrueNonPremiumState1Index;
                prio6 = indexHolder.countrySexTrueNonPremiumState2Index;
            }
            index = country;
        }

        int totalCount = 0;

        for (byte interes : account.interests) {
            long address = prio1[interes][index];
            if (address != 0) {
                int size = UNSAFE.getShort(address);
                address+=2;
                for (int i = 0; i < size; i++) {
                    int aId = UNSAFE.getInt(address);
                    address+=4;
                    Account acc = accountIdMap[aId];
                    if (city != -1) {
                        if (country != -1 && acc.country != country) {
                            continue;
                        }
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result1[result1Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            return;
        }
        for (byte interes : account.interests) {
            long address = prio2[interes][index];
            if (address != 0) {
                int size = UNSAFE.getShort(address);
                address+=2;
                for (int i = 0; i < size; i++) {
                    int aId = UNSAFE.getInt(address);
                    address+=4;
                    Account acc = accountIdMap[aId];
                    if (city != -1) {
                        if (country != -1 && acc.country != country) {
                            continue;
                        }
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result2[result2Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            return;
        }
        for (byte interes : account.interests) {
            long address = prio3[interes][index];
            if (address != 0) {
                int size = UNSAFE.getShort(address);
                address+=2;
                for (int i = 0; i < size; i++) {
                    int aId = UNSAFE.getInt(address);
                    address+=4;
                    Account acc = accountIdMap[aId];
                    if (city != -1) {
                        if (country != -1 && acc.country != country) {
                            continue;
                        }
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result3[result3Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            sizeArr[2] = result3Size;
            return;
        }
        for (byte interes : account.interests) {
            long address = prio4[interes][index];
            if (address != 0) {
                int size = UNSAFE.getShort(address);
                address+=2;
                for (int i = 0; i < size; i++) {
                    int aId = UNSAFE.getInt(address);
                    address+=4;
                    Account acc = accountIdMap[aId];
                    if (city != -1) {
                        if (country != -1 && acc.country != country) {
                            continue;
                        }
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result4[result4Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            sizeArr[2] = result3Size;
            sizeArr[3] = result4Size;
            return;
        }
        for (byte interes : account.interests) {
            long address = prio5[interes][index];
            if (address != 0) {
                int size = UNSAFE.getShort(address);
                address+=2;
                for (int i = 0; i < size; i++) {
                    int aId = UNSAFE.getInt(address);
                    address+=4;
                    Account acc = accountIdMap[aId];
                    if (city != -1) {
                        if (country != -1 && acc.country != country) {
                            continue;
                        }
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result5[result5Size++] = acc;
                    totalCount++;
                }
            }
        }
        if (totalCount >= limit) {
            sizeArr[0] = result1Size;
            sizeArr[1] = result2Size;
            sizeArr[2] = result3Size;
            sizeArr[3] = result4Size;
            sizeArr[4] = result5Size;
            return;
        }
        for (byte interes : account.interests) {
            long address = prio6[interes][index];
            if (address != 0) {
                int size = UNSAFE.getShort(address);
                address+=2;
                for (int i = 0; i < size; i++) {
                    int aId = UNSAFE.getInt(address);
                    address+=4;
                    Account acc = accountIdMap[aId];
                    if (city != -1) {
                        if (country != -1 && acc.country != country) {
                            continue;
                        }
                    }
                    byte cnt = ++recommend[aId];
                    if (cnt > 1) {
                        continue;
                    }
                    result6[result6Size++] = acc;
                    totalCount++;
                }
            }
        }
        sizeArr[0] = result1Size;
        sizeArr[1] = result2Size;
        sizeArr[2] = result3Size;
        sizeArr[3] = result4Size;
        sizeArr[4] = result5Size;
        sizeArr[5] = result6Size;
    }

    private static int fillList(Account account, Score[] result, int limit, Account[] result1, Account[] result2, Account[] result3, Account[] result4, Account[] result5, Account[] result6, int[] sizeArr, byte[] recommend) {
        int size = 0;
        Score[] pool = ObjectPool.acquireScore();
        int counter = 0;
        int res1Cnt = sizeArr[0];
        for (int i = 0; i < res1Cnt; i++) {
            Score score = pool[counter++];
            calculateScore(score, 16000, account, result1[i], recommend);
            result[size++] = score;
        }
        if (size < limit) {
            int cnt = sizeArr[1];
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 14000, account, result2[i], recommend);
                result[size++] = score;
            }
        }
        if (size < limit) {
            int cnt = sizeArr[2];
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 12000, account, result3[i], recommend);
                result[size++] = score;
            }
        }
        if (size < limit) {
            int cnt = sizeArr[3];
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 6000, account, result4[i], recommend);
                result[size++] = score;
            }
        }
        if (size < limit) {
            int cnt = sizeArr[4];
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 4000, account, result5[i], recommend);
                result[size++] = score;
            }
        }
        if (size < limit) {
            int cnt = sizeArr[5];
            for (int i = 0; i < cnt; i++) {
                Score score = pool[counter++];
                calculateScore(score, 2000, account, result6[i], recommend);
                result[size++] = score;
            }
        }
        return size;
    }

    private static ThreadLocal<AccountService.Similarity[]> similarityListPool = new ThreadLocal<AccountService.Similarity[]>() {
        @Override
        protected AccountService.Similarity[] initialValue() {
            AccountService.Similarity[] array =   new AccountService.Similarity[10000];
            for (int i = 0; i < 10000; i++) {
                array[i] =new AccountService.Similarity();
            }
            return array;
        }
    };

    private static ThreadLocal<TIntHashSet> suggestIntSet = new ThreadLocal<TIntHashSet>() {
        @Override
        protected TIntHashSet initialValue() {
            return new TIntHashSet();
        }
    };

    private static ThreadLocal<TIntDoubleMap> similarityMap = new ThreadLocal<TIntDoubleMap>() {
        @Override
        protected TIntDoubleMap initialValue() {
            return new TIntDoubleHashMap();
        }
    };

    public List<Account> suggest(int id, byte country, int city, int limit) {
        Account account = accountIdMap[id];
        if (account == null) {
            throw NotFoundRequest.INSTANCE;
        }
        if (account.likes == null || account.likes.length == 0) {
            return Collections.EMPTY_LIST;
        }

        if (country == 0 || city == 0) {
            return Collections.EMPTY_LIST;
        }

        boolean targetSex = !account.sex;

        TIntHashSet myLikes = suggestIntSet.get();
        myLikes.clear();
        double[] suggests = suggestSet.get();
        Similarity[] suggestResult = similarityListPool.get();
        int totalSize = fillSuggestResult2(account, myLikes, country, city, suggestResult, suggests);

        List<Account> result = null;
        if (totalSize == 0) {
            return Collections.EMPTY_LIST;
        }
        result = ObjectPool.acquireSuggestList();
        sortAndFetchFromSimilar(result, suggestResult, totalSize, targetSex, limit, myLikes);
        return result;
    }

    private int fillSuggestResult2(Account account, TIntHashSet myLikes, byte country, int city, Similarity[] suggestResult, double[] suggests) {
        int myId = account.id;
        int totalSize = 0;
        for (int i = 0; i < account.likes.length; i++) {
            int lid = (int)(account.likes[i] >> 32);
            if (lid == myId) {
                continue;
            }
            myLikes.add(lid);
            double sum = (int)account.likes[i];
            int cnt = 1;
            i++;
            while (i < account.likes.length) {
                int nextLike =  (int)(account.likes[i] >> 32);
                if (nextLike != lid) {
                    i--;
                    break;
                }
                sum += (int)account.likes[i];
                cnt++;
                i++;
            }
            double avgTimestamp = sum/cnt;
            long address = indexHolder.likesIndex[lid];
            long nextLike = 0;
            if (address != 0) {
                int size = UNSAFE.getByte(address);
                address++;
                nextLike = UNSAFE.getLong(address);
                for (int j = 0; j < size; j++) {
                    long like = nextLike;
                    address+=8;
                    int accId = (int)(like >> 32);
                    Account acc = accountIdMap[accId];
                    if (acc.sex != account.sex) {
                        if (j < size - 1) {
                            nextLike = UNSAFE.getLong(address);
                        }
                        continue;
                    }
                    if (country != -1 && acc.country != country) {
                        if (j < size - 1) {
                            nextLike = UNSAFE.getLong(address);
                        }
                        continue;
                    }
                    if (city != -1 && acc.city != city) {
                        if (j < size - 1) {
                            nextLike = UNSAFE.getLong(address);
                        }
                        continue;
                    }
                    int timestamp = (int)like;
                    double sum2 = timestamp;
                    int cnt2 = 1;
                    j++;
                    while ( j < size) {
                        nextLike = UNSAFE.getLong(address);
                        address+=8;
                        int nextId = (int)(nextLike >> 32);
                        int nextTimestamp = (int)nextLike;
                        if (nextId != accId) {
                            address-=8;
                            j--;
                            break;
                        }
                        sum2+=nextTimestamp;
                        cnt2++;
                        j++;
                    }
                    double avgTimestamp2 = sum2/cnt2;
                    double delta = 0;
                    if (avgTimestamp2 > avgTimestamp) {
                        delta = 1/(avgTimestamp2 - avgTimestamp);
                    } else if (avgTimestamp2 < avgTimestamp) {
                        delta = 1/(avgTimestamp - avgTimestamp2);
                    } else {
                        delta = 1;
                    }
                    double current = suggests[accId];
                    if (current == 0) {
                        Similarity similarity = suggestResult[totalSize++];
                        similarity.account = acc;
                        suggests[accId] = delta;
                    } else {
                        suggests[accId] = current + delta;
                    }
                }
            }
        }

        for (int i = 0; i < totalSize; i++) {
            Similarity similarity = suggestResult[i];
            similarity.similarity = suggests[similarity.account.id];
            suggests[similarity.account.id] = 0;
        }

        return totalSize;
    }

    private void sortAndFetchFromSimilar(List<Account> result, Similarity[] suggestResult, int totalSize, boolean targetSex, int limit, TIntHashSet myLikes) {
        TIntSet likersSet = new TIntHashSet();
        double maxSimilarity = Double.MAX_VALUE;
        while (true) {
            Similarity s = findNextMaxSimilarity(suggestResult, totalSize, maxSimilarity);
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

    public double getSimilarity(Account a1, Account a2) {
        int index1 = 0;
        int index2 = 0;
        double similarity = 0;
        int len1 = a1.likes.length;
        int len2 = a2.likes.length;
        while (index1 < len1 && index2 < len2) {
            int like1 = (int)(a1.likes[index1] >> 32);
            int like2 = (int)(a2.likes[index2] >> 32);
            if (like1 == like2) {
                double sum1 = (int)a1.likes[index1];
                double sum2 = (int)a2.likes[index2];
                int cnt1 = 1;
                int cnt2 = 1;
                index1++;
                index2++;
                while (index1 < len1 && (int)(a1.likes[index1] >> 32) == like1) {
                    sum1+=(int)a1.likes[index1];
                    cnt1++;
                    index1++;
                }
                while (index2 < len2 && (int)(a2.likes[index2] >> 32) == like2) {
                    sum2+=(int)a2.likes[index2];
                    cnt2++;
                    index2++;
                }
                double t1 = sum1/cnt1;
                double t2 = sum2/cnt2;
                if (t1 == t2) {
                    similarity+=1;
                } else {
                    if (t1 > t2) {
                        similarity += 1 / (t1 - t2);
                    } else {
                        similarity += 1 / (t2 - t1);
                    }
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
        emails.add(calculateHash(account.email, 0 , account.email.length));
        if (account.phone != null) {
            phones.add(calculateHash(account.phone, 0, account.phone.length));
        }
    }

    public synchronized void loadSequentially(Account account) {
        accountList[size] = account;
        size++;
        accountIdMap[account.id] =  account;
        emails.add(calculateHash(account.email, 0 , account.email.length));
        if (account.phone != null) {
            phones.add(calculateHash(account.phone, 0, account.phone.length));
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


    public synchronized void addValidate(AccountDTO accountDTO) {
        if (accountDTO.id == -1 || accountDTO.email == null || accountDTO.sex == null || accountDTO.birth == Integer.MIN_VALUE || accountDTO.joined == Integer.MIN_VALUE || accountDTO.status == null) {
            throw new BadRequest();
        }
        if (!ALLOWED_SEX.contains(accountDTO.sex)) {
            throw new BadRequest();
        }
        if (!ALLOWED_STATUS.contains(accountDTO.status)) {
            throw new BadRequest();
        }
        if (!contains(accountDTO.email, (byte) '@')) {
            throw new BadRequest();
        }
        if (accountIdMap[accountDTO.id] != null) {
            throw new BadRequest();
        }
        if (emails.contains(calculateHash(accountDTO.email, 0, accountDTO.email.length))) {
            throw new BadRequest();
        }
        if (accountDTO.phone != null && phones.contains(calculateHash(accountDTO.phone, 0, accountDTO.phone.length))) {
            throw new BadRequest();
        }
    }


    public synchronized void add(AccountDTO accountDTO) {
        Account account = accountConverter.convert(accountDTO);
        this.load(account);
        if (account.likes != null && account.likes.length != 0) {
            for (int j = 0; j < account.likes.length; j++) {
                int id = (int)(account.likes[j] >> 32);
                long like = 0;
                like = (long)account.id << 32;
                like |= (int)account.likes[j];
                addUserToLikeIndex(account.id, id, like);
            }
        }
        addAccountToGroups(account);
        if (account.city != 0 && account.interests != null) {
            for (byte interes: account.interests) {
                addToRecommendCityIndex(account.sex, account.premium, account.status, interes, account.city, account.id);
            }
        }
        if (account.country != 0 && account.interests != null) {
            for (byte interes: account.interests) {
                addToRecommendCountryIndex(account.sex, account.premium, account.status, interes, account.country, account.id);
            }
        }
    }

    private void addAccountToGroups(Account account) {
        addToJoinedSexIndex(account);
        addToJoinedStatusIndex(account);
        addToBirthIndex(account);
        addToBirthSexIndex(account);
        addToBirthStatusIndex(account);
        addToInteresIndex(account);
        addToInteresJoinedIndex(account);
        addToInteresBirthIndex(account);
        addToCountryIndex(account);
        addToCountryJoinedIndex(account);
        addToCountryBirthIndex(account);
        addToCityIndex(account);
        addToCityBirthIndex(account);
        addToCityJoinedIndex(account);
    }

    private void removeAccountFromGroups(Account account) {
        removeFromJoinedSexIndex(account);
        removeFromJoinedStatusIndex(account);
        removeFromBirthIndex(account);
        removeFromBirthSexIndex(account);
        removeFromBirthStatusIndex(account);
        removeFromInteresIndex(account);
        removeFromInteresJoinedIndex(account);
        removeFromInteresBirthIndex(account);
        removeFromCountryIndex(account);
        removeFromCountryJoinedIndex(account);
        removeFromCountryBirthIndex(account);
        removeFromCityIndex(account);
        removeFromCityBirthIndex(account);
        removeFromCityJoinedIndex(account);
    }



    private void addToJoinedSexIndex(Account account) {
        int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
        if (account.sex) {
            indexNumber+=7;
        }
        long[] index = indexHolder.joinedSexGroupsIndex[indexNumber];
        addAccountToGroups(account, index);
    }

    private void addToJoinedStatusIndex(Account account) {
        int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
        indexNumber+=account.status*7;
        long[] index = indexHolder.joinedStatusGroupsIndex[indexNumber];
        addAccountToGroups(account, index);
    }

    private void addToBirthIndex(Account account) {
        int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
        long[] index = indexHolder.birthGroupsIndex[indexNumber];
        addAccountToGroups(account, index);
    }

    private void addToBirthSexIndex(Account account) {
        int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
        if (account.sex) {
            indexNumber+=55;
        }
        long[] index = indexHolder.birthSexGroupsIndex[indexNumber];
        addAccountToGroups(account, index);
    }

    private void addToBirthStatusIndex(Account account) {
        int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
        indexNumber+=55*account.status;
        long[] index = indexHolder.birthStatusGroupsIndex[indexNumber];
        addAccountToGroups(account, index);
    }

    private void addToInteresIndex(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            for (byte interes: account.interests) {
                long[] index = indexHolder.interesGroupsIndex[interes - 1];
                addAccountToGroups(account, index);
            }
        }
    }

    private void addToInteresJoinedIndex(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
            for (byte interes: account.interests) {
                long[] index = indexHolder.interesJoinedGroupsIndex[indexNumber*90 + interes - 1];
                addAccountToGroups(account, index);
            }
        }
    }

    private void addToInteresBirthIndex(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
            for (byte interes: account.interests) {
                long[] index = indexHolder.interesBirthGroupsIndex[indexNumber*90 + interes - 1];
                addAccountToGroups(account, index);
            }
        }
    }

    private void addToCountryIndex(Account account) {
        if (account.country != 0) {
            long[] index = indexHolder.countryGroupsIndex[account.country - 1];
            addAccountToGroups(account, index);
        }
    }

    private void addToCountryJoinedIndex(Account account) {
        if (account.country != 0) {
            int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
            long[] index = indexHolder.countryJoinedGroupsIndex[indexNumber*70 + account.country - 1];
            addAccountToGroups(account, index);
        }
    }

    private void addToCountryBirthIndex(Account account) {
        if (account.country != 0) {
            int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
            long[] index = indexHolder.countryBirthGroupsIndex[indexNumber*70 + account.country - 1];
            addAccountToGroups(account, index);
        }
    }

    private void addToCityIndex(Account account) {
        if (account.city != 0) {
            long[] index = indexHolder.cityGroupsIndex[account.city - 1];
            addAccountToGroups(account, index);
        }
    }

    private void addToCityBirthIndex(Account account) {
        if (account.city != 0) {
            int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
            long[] index = indexHolder.cityBirthGroupsIndex[indexNumber*700 + account.city - 1];
            addAccountToGroups(account, index);
        }
    }

    private void addToCityJoinedIndex(Account account) {
        if (account.city != 0) {
            int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
            long[] index = indexHolder.cityJoinedGroupsIndex[indexNumber*700 + account.city - 1];
            addAccountToGroups(account, index);
        }
    }

    private void removeFromJoinedSexIndex(Account account) {
        int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
        if (account.sex) {
            indexNumber+=7;
        }
        long[] index = indexHolder.joinedSexGroupsIndex[indexNumber];
        removeAccountFromGroups(account, index);
    }

    private void removeFromJoinedStatusIndex(Account account) {
        int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
        indexNumber+=account.status*7;
        long[] index = indexHolder.joinedStatusGroupsIndex[indexNumber];
        removeAccountFromGroups(account, index);
    }

    private void removeFromBirthIndex(Account account) {
        int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
        long[] index = indexHolder.birthGroupsIndex[indexNumber];
        removeAccountFromGroups(account, index);
    }

    private void removeFromBirthSexIndex(Account account) {
        int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
        if (account.sex) {
            indexNumber+=55;
        }
        long[] index = indexHolder.birthSexGroupsIndex[indexNumber];
        removeAccountFromGroups(account, index);
    }

    private void removeFromBirthStatusIndex(Account account) {
        int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
        indexNumber+=55*account.status;
        long[] index = indexHolder.birthStatusGroupsIndex[indexNumber];
        removeAccountFromGroups(account, index);
    }

    private void removeFromInteresIndex(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            for (byte interes: account.interests) {
                long[] index = indexHolder.interesGroupsIndex[interes - 1];
                removeAccountFromGroups(account, index);
            }
        }
    }

    private void removeFromInteresJoinedIndex(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
            for (byte interes: account.interests) {
                long[] index = indexHolder.interesJoinedGroupsIndex[indexNumber*90 + interes - 1];
                removeAccountFromGroups(account, index);
            }
        }
    }

    private void removeFromInteresBirthIndex(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
            for (byte interes: account.interests) {
                long[] index = indexHolder.interesBirthGroupsIndex[indexNumber*90 + interes - 1];
                removeAccountFromGroups(account, index);
            }
        }
    }

    private void removeFromCountryIndex(Account account) {
        if (account.country != 0) {
            long[] index = indexHolder.countryGroupsIndex[account.country - 1];
            removeAccountFromGroups(account, index);
        }
    }

    private void removeFromCountryJoinedIndex(Account account) {
        if (account.country != 0) {
            int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
            long[] index = indexHolder.countryJoinedGroupsIndex[indexNumber*70 + account.country - 1];
            removeAccountFromGroups(account, index);
        }
    }

    private void removeFromCountryBirthIndex(Account account) {
        if (account.country != 0) {
            int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
            long[] index = indexHolder.countryBirthGroupsIndex[indexNumber*70 + account.country - 1];
            removeAccountFromGroups(account, index);
        }
    }

    private void removeFromCityIndex(Account account) {
        if (account.city != 0) {
            long[] index = indexHolder.cityGroupsIndex[account.city - 1];
            removeAccountFromGroups(account, index);
        }
    }

    private void removeFromCityBirthIndex(Account account) {
        if (account.city != 0) {
            int indexNumber = BirthYearPredicate.calculateYear(account.birth) - 1950;
            long[] index = indexHolder.cityBirthGroupsIndex[indexNumber*700 + account.city - 1];
            removeAccountFromGroups(account, index);
        }
    }

    private void removeFromCityJoinedIndex(Account account) {
        if (account.city != 0) {
            int indexNumber = JoinedYearPredicate.calculateYear(account.joined) - 2011;
            long[] index = indexHolder.cityJoinedGroupsIndex[indexNumber*700 + account.city - 1];
            removeAccountFromGroups(account, index);
        }
    }

    private void addAccountToGroups(Account account, long[] index) {
        for (int i = 1; i < 9;i++) {
            int group = getGroup(account,IndexHolder.masks[i]);
            incrementGroup(group, index, i);
        }
        if (account.interests != null && account.interests.length != 0) {
            for (byte interes: account.interests) {
                int group = 0;
                group |= interes << 20;
                incrementGroup(group, index, 0);
            }
        }
    }

    private void removeAccountFromGroups(Account account, long[] index) {
        for (int i = 1; i < 9;i++) {
            int group = getGroup(account,IndexHolder.masks[i]);
            decrementGroup(group, index, i);
        }
        if (account.interests != null && account.interests.length != 0) {
            for (byte interes: account.interests) {
                int group = 0;
                group |= interes << 20;
                decrementGroup(group, index, 0);
            }
        }
    }

    private int getGroup(Account account, byte keyMask) {
        int group = 0;
        if ((keyMask & 0b00000001) != 0) {
            if (account.sex) {
                group|=1;
            }
        }
        if ((keyMask & 0b00000010) != 0) {
            group|=account.status << 1;
        }
        if ((keyMask & 0b00001000) != 0) {
            group|=account.country << 3;
        }
        if ((keyMask & 0b00010000) != 0) {
            group|=account.city << 10;
        }
        return group;
    }

    private void incrementGroup(int group, long[] index, int indexNumber) {
        if (index[indexNumber] == 0) {
            long address = UNSAFE.allocateMemory(10);
            index[indexNumber] = address;
            UNSAFE.putShort(address, (short)1);
            long value = ((long)group << 32) | 1;
            UNSAFE.putLong(address+2, value);
        } else {
            long address = index[indexNumber];
            int count = UNSAFE.getShort(address);
            address+=2;
            for (int i = 0; i < count; i++) {
                long value = UNSAFE.getLong(address);
                int grp = (int) (value >>> 32);
                if (grp == group) {
                    value++;
                    UNSAFE.putLong(address, value);
                    long position = address;
                    while (position != index[indexNumber] + 2 + (count - 1)*8) {
                        long value1 = UNSAFE.getLong(position);
                        long value2 = UNSAFE.getLong(position + 8);
                        int count1 = (int) value1;
                        int group1 = (int)(value1 >>> 32);
                        int count2 = (int) value2;
                        int group2 = (int)(value2 >>> 32);
                        int cc = compare(count1, group1, count2, group2, IndexHolder.keys[indexNumber], 1);
                        if (cc > 0) {
                            UNSAFE.putLong(position, value2);
                            UNSAFE.putLong(position + 8, value1);
                        } else {
                            break;
                        }
                        position+=8;
                    }
                    return;
                }
                address+=8;
            }
            long oldAddress = index[indexNumber];
            long newAddress = UNSAFE.allocateMemory(2 + (count +1)*8);
            index[indexNumber] = newAddress;
            UNSAFE.copyMemory(oldAddress, newAddress, 2 +count*8);
            UNSAFE.putShort(newAddress, (short)(count + 1));
            long newValue = ((long)group << 32) | 1;
            UNSAFE.putLong(newAddress + 2 + count*8, newValue);
            long position = newAddress + 2 + count*8;
            while (position != newAddress + 2) {
                long value1 = UNSAFE.getLong(position - 8);
                long value2 = UNSAFE.getLong(position);
                int count1 = (int) value1;
                int group1 = (int)(value1 >>> 32);
                int count2 = (int) value2;
                int group2 = (int)(value2 >>> 32);
                int cc = compare(count1, group1, count2, group2, IndexHolder.keys[indexNumber], 1);
                if (cc > 0) {
                    UNSAFE.putLong(position, value1);
                    UNSAFE.putLong(position - 8, value2);
                } else {
                    break;
                }
                position-=8;
            }
            UNSAFE.freeMemory(oldAddress);
        }
    }

    private void decrementGroup(int group, long[] index, int indexNumber) {
        if (index[indexNumber] != 0) {
            long address = index[indexNumber];
            int count = UNSAFE.getShort(address);
            address+=2;
            for (int i = 0; i < count; i++) {
                long value = UNSAFE.getLong(address);
                int grp = (int) (value >>> 32);
                if (grp == group) {
                    int cnt = (int)(value);
                    if (cnt != 1) {
                        value--;
                        UNSAFE.putLong(address, value);
                        long position = address;
                        while (position != index[indexNumber] + 2) {
                            long value1 = UNSAFE.getLong(position - 8);
                            long value2 = UNSAFE.getLong(position);
                            int count1 = (int) value1;
                            int group1 = (int)(value1 >>> 32);
                            int count2 = (int) value2;
                            int group2 = (int)(value2 >>> 32);
                            int cc = compare(count1, group1, count2, group2, IndexHolder.keys[indexNumber], 1);
                            if (cc > 0) {
                                UNSAFE.putLong(position, value1);
                                UNSAFE.putLong(position - 8, value2);
                            } else {
                                break;
                            }
                            position-=8;
                        }
                    } else {
                        if (count == 1) {
                            UNSAFE.freeMemory(index[indexNumber]);
                            index[indexNumber] = 0;
                        } else {
                            long newAddress = UNSAFE.allocateMemory(2 + (count - 1) * 8);
                            UNSAFE.putShort(newAddress, (short)(count - 1));
                            long newPointer = newAddress + 2;
                            long oldPointer = index[indexNumber] + 2;
                            for (int j = 0; j < count; j++) {
                                if (j != i) {
                                    UNSAFE.putLong(newPointer, UNSAFE.getLong(oldPointer));
                                    newPointer+=8;
                                }
                                oldPointer+=8;
                            }
                            UNSAFE.freeMemory(index[indexNumber]);
                            index[indexNumber] = newAddress;
                        }
                    }
                    return;
                }
                address+=8;
            }
        }
    }

    public synchronized void updateValidate(AccountDTO accountDTO) {
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
        if (accountDTO.email != null && !contains(accountDTO.email,(byte) '@')) {
            throw new BadRequest();
        }
        if (accountDTO.status != null) {
            ConvertorUtills.convertStatusNumber(accountDTO.status);
        }
        if (accountDTO.email != null && !Arrays.equals(oldAcc.email,accountDTO.email)) {
            if (emails.contains(calculateHash(accountDTO.email, 0,  accountDTO.email.length))) {
                throw new BadRequest();
            }
        }
        if ((accountDTO.phone != null && oldAcc.phone == null) ||( accountDTO.phone != null && !Arrays.equals(oldAcc.phone, accountDTO.phone))) {
            if (phones.contains(calculateHash(accountDTO.phone, 0,  accountDTO.phone.length))) {
                throw new BadRequest();
            }
        }
    }

    public synchronized void update(AccountDTO accountDTO) {
        Account oldAcc = accountIdMap[accountDTO.id];
        if (accountDTO.email != null && !Arrays.equals(oldAcc.email, accountDTO.email)) {
            emails.remove(calculateHash(oldAcc.email, 0, oldAcc.email.length));
            emails.add(calculateHash(accountDTO.email, 0, accountDTO.email.length));
            oldAcc.email = accountDTO.email;
        }
        if (accountDTO.sex != null || accountDTO.interests != null || accountDTO.premiumStart != 0 || accountDTO.status != null || accountDTO.city != null) {
            if (oldAcc.city != 0 && oldAcc.interests != null) {
                for (byte interes: oldAcc.interests) {
                    removeFromRecommendCityIndex(oldAcc.sex, oldAcc.premium, oldAcc.status, interes, oldAcc.city, oldAcc.id);
                }
            }
        }
        if (accountDTO.sex != null || accountDTO.interests != null || accountDTO.premiumStart != 0 || accountDTO.status != null || accountDTO.country != null) {
            if (oldAcc.country != 0 && oldAcc.interests != null) {
                for (byte interes: oldAcc.interests) {
                    removeFromRecommendCountryIndex(oldAcc.sex, oldAcc.premium, oldAcc.status, interes, oldAcc.country, oldAcc.id);
                }
            }
        }
        if (accountDTO.sex != null || accountDTO.interests != null || accountDTO.country != null || accountDTO.status != null || accountDTO.city != null || accountDTO.joined != 0 || accountDTO.birth != 0) {
            removeAccountFromGroups(oldAcc);
        }
        if (accountDTO.sex != null) {
            oldAcc.sex = ConvertorUtills.convertSex(accountDTO.sex);
        }
        if (accountDTO.fname != null) {
            oldAcc.fname = dictionary.getOrCreateFname(accountDTO.fname);
            dictionary.updateFnameSexDictionary(oldAcc.fname, oldAcc.sex);
        }
        if (accountDTO.sname != null) {
            oldAcc.sname = dictionary.getOrCreateSname(accountDTO.sname);
        }
        if (accountDTO.phone != null && oldAcc.phone != null && !Arrays.equals(oldAcc.phone, accountDTO.phone)) {
            phones.remove(calculateHash(oldAcc.phone, 0, oldAcc.phone.length));
            phones.add(calculateHash(accountDTO.phone, 0, accountDTO.phone.length));
            oldAcc.phone = accountDTO.phone;
        } else if (oldAcc.phone == null && accountDTO.phone != null) {
            phones.add(calculateHash(accountDTO.phone, 0, accountDTO.phone.length));
            oldAcc.phone = accountDTO.phone;
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
        if (accountDTO.sex != null || accountDTO.interests != null || accountDTO.country != null || accountDTO.status != null || accountDTO.city != null || accountDTO.joined != 0 || accountDTO.birth != 0) {
            addAccountToGroups(oldAcc);
        }
        if (accountDTO.sex != null || accountDTO.interests != null || accountDTO.premiumStart != 0 || accountDTO.status != null || accountDTO.city != null) {
            if (oldAcc.city != 0 && oldAcc.interests != null) {
                for (byte interes: oldAcc.interests) {
                    addToRecommendCityIndex(oldAcc.sex, oldAcc.premium, oldAcc.status, interes, oldAcc.city, oldAcc.id);
                }
            }
        }
        if (accountDTO.sex != null || accountDTO.interests != null || accountDTO.premiumStart != 0 || accountDTO.status != null || accountDTO.country != null) {
            if (oldAcc.country != 0 && oldAcc.interests != null) {
                for (byte interes: oldAcc.interests) {
                    addToRecommendCountryIndex(oldAcc.sex, oldAcc.premium, oldAcc.status, interes, oldAcc.country, oldAcc.id);
                }
            }
        }
    }

    private long calculateHash(byte[] values, int from, int to) {
        long hash = 0;
        for (int i = from; i < to; i++) {
            hash = 31 * hash + values[i];
        }
        return hash;
    }

    public synchronized void likeValidate(List<LikeRequest> likeRequests) {
        for (int i = 0; i < likeRequests.size(); i++) {
            LikeRequest likeRequest = likeRequests.get(i);
            if (likeRequest.likee >= MAX_ID || likeRequest.liker >= MAX_ID) {
                throw BadRequest.INSTANCE;
            }
            if (likeRequest.likee == -1 || likeRequest.liker == -1 || likeRequest.ts == -1 || accountIdMap[likeRequest.likee] == null || accountIdMap[likeRequest.liker] == null) {
                throw BadRequest.INSTANCE;
            }
        }
    }

    public synchronized void like(List<LikeRequest> likeRequests) {
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
                like = 0;
                like = (long) likeRequest.liker << 32;
                like = (likeRequest.ts | like);
                addUserToLikeIndex(likeRequest.liker, likeRequest.likee, like);
            }
    }

    private void addUserToLikeIndex(int liker, int likee, long like) {
        long address = indexHolder.likesIndex[likee];
        if (address == 0) {
            address = UNSAFE.allocateMemory(9);
            UNSAFE.putByte(address, (byte)1);
            UNSAFE.putLong(address + 1, like);
            indexHolder.likesIndex[likee] = address;
        } else {
            int size = UNSAFE.getByte(address);
            int insertId = size;
            boolean dontInsert = false;
            long pointer = address + 5;
            for (int i = 0; i < size; i++) {
                int id = UNSAFE.getInt(pointer);
                pointer += 8;
                if (id < liker) {
                    insertId = i;
                    break;
                }
            }
            long newAddress = UNSAFE.allocateMemory(1 + (size + 1) * 8);
            UNSAFE.putByte(newAddress, (byte) (size + 1));
            if (insertId == 0) {
                UNSAFE.copyMemory(address + 1, newAddress + 1 + 8, size * 8);
                UNSAFE.putLong(newAddress + 1, like);
            } else if (insertId == size) {
                UNSAFE.copyMemory(address + 1, newAddress + 1, size * 8);
                UNSAFE.putLong(newAddress + 1 + 8 * size, like);
            } else {
                UNSAFE.copyMemory(address + 1, newAddress + 1, insertId * 8);
                UNSAFE.copyMemory(address + 1 + insertId * 8, newAddress + 1 + (insertId + 1) * 8, (size - insertId) * 8);
                UNSAFE.putLong(newAddress + 1 + 8 * insertId, like);
            }
            indexHolder.likesIndex[likee] = newAddress;
            UNSAFE.freeMemory(address);
        }
    }

    private void removeFromRecommendCityIndex(boolean sex, boolean premium, int state, byte interes, int city, int id) {
        long[][] index;
        if (sex) {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.citySexTruePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexTruePremiumState1Index;
                } else {
                    index = indexHolder.citySexTruePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.citySexTrueNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexTrueNonPremiumState1Index;
                } else {
                    index = indexHolder.citySexTrueNonPremiumState2Index;
                }
            }
        } else {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.citySexFalsePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexFalsePremiumState1Index;
                } else {
                    index = indexHolder.citySexFalsePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.citySexFalseNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexFalseNonPremiumState1Index;
                } else {
                    index = indexHolder.citySexFalseNonPremiumState2Index;
                }
            }
        }
        removeAccount(index, interes, city, id);
    }


    private void removeFromRecommendCountryIndex(boolean sex, boolean premium, int state, byte interes, int country, int id) {
        long[][] index;
        if (sex) {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.countrySexTruePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexTruePremiumState1Index;
                } else {
                    index = indexHolder.countrySexTruePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.countrySexTrueNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexTrueNonPremiumState1Index;
                } else {
                    index = indexHolder.countrySexTrueNonPremiumState2Index;
                }
            }
        } else {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.countrySexFalsePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexFalsePremiumState1Index;
                } else {
                    index = indexHolder.countrySexFalsePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.countrySexFalseNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexFalseNonPremiumState1Index;
                } else {
                    index = indexHolder.countrySexFalseNonPremiumState2Index;
                }
            }
        }
        removeAccount(index, interes, country, id);
    }

    private void addToRecommendCityIndex(boolean sex, boolean premium, int state, byte interes, int city, int id) {
        long[][] index;
        if (sex) {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.citySexTruePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexTruePremiumState1Index;
                } else {
                    index = indexHolder.citySexTruePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.citySexTrueNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexTrueNonPremiumState1Index;
                } else {
                    index = indexHolder.citySexTrueNonPremiumState2Index;
                }
            }
        } else {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.citySexFalsePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexFalsePremiumState1Index;
                } else {
                    index = indexHolder.citySexFalsePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.citySexFalseNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.citySexFalseNonPremiumState1Index;
                } else {
                    index = indexHolder.citySexFalseNonPremiumState2Index;
                }
            }
        }
        addAccount(index, interes, city, id);
    }

    private void addToRecommendCountryIndex(boolean sex, boolean premium, int state, byte interes, byte country, int id) {
        long[][] index;
        if (sex) {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.countrySexTruePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexTruePremiumState1Index;
                } else {
                    index = indexHolder.countrySexTruePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.countrySexTrueNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexTrueNonPremiumState1Index;
                } else {
                    index = indexHolder.countrySexTrueNonPremiumState2Index;
                }
            }
        } else {
            if (premium) {
                if (state == 0) {
                    index = indexHolder.countrySexFalsePremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexFalsePremiumState1Index;
                } else {
                    index = indexHolder.countrySexFalsePremiumState2Index;
                }

            } else {
                if (state == 0) {
                    index = indexHolder.countrySexFalseNonPremiumState0Index;
                } else if (state == 1) {
                    index = indexHolder.countrySexFalseNonPremiumState1Index;
                } else {
                    index = indexHolder.countrySexFalseNonPremiumState2Index;
                }
            }
        }
        addAccount(index, interes, country, id);
    }

    private void addAccount(long[][] index, byte interes, int city, int id) {
        long address = index[interes][city];
        if (address == 0) {
            address = UNSAFE.allocateMemory(6);
            UNSAFE.putShort(address, (short)1);
            UNSAFE.putInt(address + 2, id);
            index[interes][city] = address;
        } else {
            int oldSize = UNSAFE.getShort(address);
            int newSize = oldSize + 1;
            long newAddress = UNSAFE.allocateMemory(2 + newSize*4);
            UNSAFE.copyMemory(address + 2, newAddress + 2, oldSize*4);
            UNSAFE.putInt(newAddress + 2+ oldSize*4, id);
            UNSAFE.putShort(newAddress, (short) newSize);
            index[interes][city] = newAddress;
            UNSAFE.freeMemory(address);
        }
    }

    private void removeAccount(long[][] index, byte interes, int city, int id) {
        long address = index[interes][city];
        int oldSize = UNSAFE.getShort(address);
        if (oldSize == 1) {
            UNSAFE.freeMemory(address);
            index[interes][city] = 0;
        } else {
            long newAddress = UNSAFE.allocateMemory(2 + (oldSize-1)*4);
            UNSAFE.putShort(newAddress, (short) (oldSize - 1));
            long position = 0;
            for (int i = 0; i < oldSize; i++) {
                int item = UNSAFE.getInt(address + 2 + i*4);
                if (item == id) {
                    position = i;
                }
            }
            if (position == 0) {
                UNSAFE.copyMemory(address + 6, newAddress + 2, (oldSize-1)*4);
            } else if (position == oldSize - 1) {
                UNSAFE.copyMemory(address + 2, newAddress + 2, (oldSize-1)*4);
            } else {
                UNSAFE.copyMemory(address + 2, newAddress + 2, position*4);
                UNSAFE.copyMemory(address + 2 + position*4 + 4, newAddress + 2 + position*4, (oldSize - 1 - position)*4);
            }
            UNSAFE.freeMemory(address);
            index[interes][city] = newAddress;
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


    private List<IndexScan> getAvailableIndexScan(List<AbstractPredicate> predicates) {
        List<IndexScan> indexScans = ObjectPool.acquireIndexScanList();
        Iterator<AbstractPredicate> predicateIterator = predicates.iterator();
        AbstractPredicate usefulIndex = null;
        for (int i = 0; i < predicates.size(); i++) {
            AbstractPredicate predicate = predicates.get(i);
            if (predicate.getIndexCordiality() != Integer.MAX_VALUE) {
                if (usefulIndex == null) {
                    usefulIndex = predicate;
                } else {
                    if (predicate.getIndexCordiality() < usefulIndex.getIndexCordiality()) {
                        usefulIndex = predicate;
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

    private int andPredicates(List<AbstractPredicate> predicates, AbstractPredicate[] array) {
        predicates.toArray(array);
        return predicates.size();
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
                epollServer.suspend();
                schedule(TERMINATOR);
                System.out.println(LAST_UPDATE_TIMESTAMP);
                countDownLatch.await();
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
                for (int i = 0; i < 100000;i++) {
                    long addr = RequestHandler.cache[i];
                    if (addr != 0) {
                        UNSAFE.freeMemory(addr);
                        RequestHandler.cache[i] = 0;
                    }
                }
                RequestHandler.cache = RequestHandler.cache;
                indexHolder.init(accountList, size, false);
                //System.gc();
                System.out.println("End update indexes " + new Date());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void receivedPost() {
        if (LAST_UPDATE_TIMESTAMP == 0) {
            synchronized (this) {
                if (LAST_UPDATE_TIMESTAMP == 0) {
                    LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
                    new Thread(new IndexUpdater()).start();
                    executorThread.start();
                }
            }
        }
        LAST_UPDATE_TIMESTAMP = System.currentTimeMillis();
    }

    public static class Similarity {
        public Account account;
        public double similarity;
    }

    public static class Score {
        public Account account;
        public long score;
    }

    private static void calculateScore(Score score, int base, Account my, Account other, byte[] recommend) {
        score.account = other;
        long value = base;
        value+=recommend[other.id];
        recommend[other.id] = 0;
        int delta = my.birth - other.birth;
        if (delta < 0) {
            delta=-delta;
        }
        value = (value << 32) | (0x7fffffff - delta);
        score.score = value;
    }

    private boolean contains(byte[] values, byte ch) {
        for (int i = values.length - 1; i >= 0; i--) {
            if (values[i] == ch) {
                return true;
            }
        }
        return false;
    }

    public void schedule(Runnable runnable) {
        try {
            TASKS.put(runnable);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private BlockingQueue<Runnable> TASKS = new ArrayBlockingQueue<>(50000);

    private static final Runnable TERMINATOR = new Runnable() {
        @Override
        public void run() {
            throw new NullPointerException();
        }
    };

    private Thread executorThread = new Thread(()-> {
        try {
            while (true) {
                Runnable runnable = TASKS.take();
                if (runnable == TERMINATOR) {
                    System.out.println(TASKS.size());
                    System.out.println("Terminator="+new Date().getTime());
                    countDownLatch.countDown();
                    break;
                } else {
                    runnable.run();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    });

}
