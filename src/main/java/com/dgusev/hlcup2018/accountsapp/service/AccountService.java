package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.index.*;
import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.*;
import com.dgusev.hlcup2018.accountsapp.predicate.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class AccountService {
    public  static final int MAX_ID = 1520000;
    private static final Set<String> ALLOWED_SEX = new HashSet<>(Arrays.asList("m", "f"));
    private static final Set<String> ALLOWED_STATUS = new HashSet<>(Arrays.asList("свободны", "всё сложно","заняты"));
    private static final String EMAIL_REG = "[0-9a-zA-z]+@[0-9a-zA-z]+\\.[0-9a-zA-z]+";


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
            List<Account> result = new ArrayList<>();
            int count = 0;
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }

                Account account = findById(next);
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

        List<Account> result = new ArrayList<>();
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
        List<IndexScan> indexScans = getAvailableIndexScan(predicates);
        HashMap<Long, IntegerHolder> groupHashMap = new HashMap<>();
        HashMap<Long, List<String>> groupNameMap = new HashMap<>();
        List<String> tmpGroupList = new ArrayList<>();
        if (!indexScans.isEmpty()) {
            Predicate<Account> accountPredicate = andPredicates(predicates);
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }
                Account account = findById(next);
                if (accountPredicate.test(account)) {
                    processRecord(account, groupHashMap, groupNameMap , keys, tmpGroupList);
                }
            }
        } else {
            Predicate<Account> accountPredicate = andPredicates(predicates);
            for (int i = 0; i< size; i++) {
                Account account = accountList[i];
                if (accountPredicate.test(account)) {
                    processRecord(account, groupHashMap, groupNameMap, keys, tmpGroupList);
                }
            }
        }
        return groupHashMap.entrySet().stream().map(e -> {
            Group group = new Group();
            group.count = e.getValue().count;
            group.values = groupNameMap.get(e.getKey());
            return group;
        }).sorted((g1, g2) -> {
            if (order == 1) {
                int cc = Integer.compare(g1.count, g2.count);
                if (cc == 0) {
                    return compareGroups(g1.values, g2.values);
                } else {
                    return cc;
                }
            } else {
                int cc = Integer.compare(g2.count, g1.count);
                if (cc == 0) {
                    return compareGroups(g2.values, g1.values);
                } else {
                    return cc;
                }
            }
        }).limit(limit).collect(Collectors.toList());
    }

    private void processRecord(Account account, Map<Long, IntegerHolder> groupHashMap, Map<Long, List<String>> groupNameMap, List<String> keys, List<String> group) {
        group.clear();
        long hashcode =  0;
        for (int i = 0; i < keys.size(); i++) {
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
                for (String interes : account.interests) {
                    long newHashcode = hashcode;
                    newHashcode = 31 * newHashcode + interes.hashCode();
                    List<String> newGroup = new ArrayList<>(group);
                    newGroup.add(interes);
                    incrementGroup(groupHashMap, groupNameMap, newGroup, newHashcode);
                }
            }
        } else {
            incrementGroup(groupHashMap, groupNameMap, group, hashcode);
        }
    }

    private void incrementGroup(Map<Long, IntegerHolder> groupHashMap, Map<Long, List<String>> groupNameMap, List<String> group, Long hash) {
        if (!groupNameMap.containsKey(hash)) {
            groupNameMap.put(hash, new ArrayList<>(group));
        }
        if (!groupHashMap.containsKey(hash)) {
            groupHashMap.put(hash, new IntegerHolder());
        }
        groupHashMap.get(hash).count++;
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


    public List<Account> recommend(int id, List<Predicate<Account>> predicates, int limit) {
        if (limit <= 0) {
            throw new BadRequest();
        }
        Account account = findById(id);
        if (account == null) {
            throw new NotFoundRequest();
        }
        predicates.add(new SexEqPredicate(!account.sex));
        predicates.add(a -> a.id != id);
        predicates.add(a -> {
            if (account.interests == null || a.interests == null || account.interests.length == 0 || a.interests.length == 0) {
                return false;
            }
            if (interestsMatched(Arrays.asList(account.interests), Arrays.asList(a.interests)) == 0) {
                return false;
            }
            return true;

        });
        List<IndexScan> indexScans = getAvailableIndexScan(predicates);
        Predicate<Account> accountPredicate = andPredicates(predicates);
        Set<String> interests = new HashSet<>();
        if (account.interests != null) {
            interests.addAll(Arrays.asList(account.interests));
        }
        List<Account> result1 = new ArrayList<>();
        List<Account> result2 = new ArrayList<>();
        List<Account> result3 = new ArrayList<>();
        List<Account> result4 = new ArrayList<>();
        List<Account> result5 = new ArrayList<>();
        List<Account> result6 = new ArrayList<>();
        IndexScan indexScan = new CompositeIndexScan(indexScans);
        PremiumNowPredicate premiumNowPredicate = new PremiumNowPredicate(nowProvider.getNow());
        while (true) {
            int next = indexScan.getNext();
            if (next == -1) {
                break;
            }
            Account acc = findById(next);
            if (accountPredicate.test(acc)) {
                if (premiumNowPredicate.test(acc)) {
                    if (acc.status == 0) {
                        result1.add(acc);
                    } else if (acc.status == 1) {
                        result2.add(acc);
                    } else {
                        result3.add(acc);
                    }
                } else {
                    if (acc.status == 0) {
                        result4.add(acc);
                    } else if (acc.status == 1) {
                        result5.add(acc);
                    } else {
                        result6.add(acc);
                    }
                }
            }
        }
        List<Account> result = new ArrayList<>();
        result.addAll(result1);
        if (result.size() < limit) {
            result.addAll(result2);
        }
        if (result.size() < limit) {
            result.addAll(result3);
        }
        if (result.size() < limit) {
            result.addAll(result4);
        }
        if (result.size() < limit) {
            result.addAll(result5);
        }
        if (result.size() < limit) {
            result.addAll(result6);
        }
        result.sort((a1, a2) -> {
            if (isPremium(a1) && !isPremium(a2)) {
                return -1;
            } else if (!isPremium(a1) && isPremium(a2)) {
                return 1;
            }
            int cc1 = Integer.compare(a1.status, a2.status);
            if (cc1 != 0) {
                return cc1;
            }
            int int1 = interestsMatched(interests, Arrays.asList(a1.interests != null ? a1.interests : new String[0]));
            int int2 = interestsMatched(interests, Arrays.asList(a2.interests != null ? a2.interests : new String[0]));
            int cc2 = Integer.compare(int1, int2);
            if (cc2 != 0) {
                return -cc2;
            }
            int bd1 = Math.abs(a1.birth - account.birth);
            int bd2 = Math.abs(a2.birth - account.birth);
            int cc3 = Integer.compare(bd1, bd2);
            if (cc3 != 0) {
                return cc3;
            }
            return Integer.compare(a1.id, a2.id);
        });
        if (result.size() > limit) {
            return result.subList(0, limit);
        } else  {
            return result;
        }
    }



    private int interestsMatched(Collection<String> myInterests, Collection<String> othersInterests) {
        if (othersInterests == null || othersInterests.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (String interes: othersInterests) {
            if (myInterests.contains(interes)) {
                count++;
            }
        }
        return count;
    }

    private boolean isPremium(Account account) {
        return account.premiumStart != 0 && account.premiumStart < nowProvider.getNow() && (account.premiumFinish > nowProvider.getNow() || account.premiumFinish == 0);
    }


    public List<Account> suggest(int id, List<Predicate<Account>> predicates, int limit) {
        Account account = findById(id);
        if (account == null) {
            throw new NotFoundRequest();
        }
        predicates.add(new SexEqPredicate(account.sex));
        predicates.add(a -> a.id != id);

        boolean targetSex = !account.sex;
        if (account.likes == null || account.likes.length == 0) {
            return Collections.EMPTY_LIST;
        }
        Set<Integer> myLikes = new HashSet<>();
        for (long like: account.likes) {
            int lid = (int)(like & 0x0000ffff);
            myLikes.add(lid);
        }
        Set<Integer> suggests = new HashSet<>();
        for (Integer a: myLikes) {
            if (indexHolder.likesIndex.containsKey(a)) {
                int[] likers = indexHolder.likesIndex.get(a);
                for (int l: likers) {
                    suggests.add(l);
                }
            }
        }
        if (suggests.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        List<Integer> likersIndex = new ArrayList<>(suggests);
        likersIndex.sort(Comparator.reverseOrder());
        IndexScan likersIndexScan = new ArrayIndexScan(likersIndex.stream().mapToInt(i->i).toArray());

        List<IndexScan> indexScans = getAvailableIndexScan(predicates);
        indexScans.add(likersIndexScan);
        IndexScan indexScan = new CompositeIndexScan(indexScans);
        Predicate<Account> accountPredicate = andPredicates(predicates);
        List<Account> suggestResult = new ArrayList<>();
        while (true) {
            int next = indexScan.getNext();
            if (next == -1) {
                break;
            }
            Account acc = findById(next);
            if (accountPredicate.test(acc)) {
                suggestResult.add(acc);
            }
        }
        List<Similarity> similarities = suggestResult.stream().map(a ->  {
            Similarity similarity = new Similarity();
            similarity.account = a;
            similarity.similarity = getSimilarity(account, a);
            return similarity;
        }).sorted(Comparator.comparingDouble((Similarity s) -> s.similarity).reversed()).collect(Collectors.toList());
        List<Account> result = new ArrayList<>();
        Set<Integer> likersSet = new HashSet<>();
        for (int i = 0; i < similarities.size(); i++) {
            Similarity s = similarities.get(i);
            int[] likers = Arrays.stream(s.account.likes).mapToInt(l -> (Integer)(int)(l & 0x0000ffff)).toArray();
            Arrays.sort(likers);
            for (int j = likers.length -1; j >= 0; j--) {
                if (!myLikes.contains(likers[j]) && findById(likers[j]).sex==targetSex) {
                    if (!likersSet.contains(likers[j])) {
                        likersSet.add(likers[j]);
                        result.add(accountIdMap[likers[j]]);
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
    }

    private double getSimilarity(Account a1, Account a2) {

        List<Like> like1 = a1.likes != null ? Arrays.stream( a1.likes).mapToObj(l -> {
            Like like = new Like();
            like.id = (int)(l & 0x00000000ffffffffL);
            like.ts = (int)(l >> 32);
            return like;
        }).collect(Collectors.toList()) : new ArrayList<>();
        List<Like> like2 = a2.likes != null ? Arrays.stream( a2.likes).mapToObj(l -> {
            Like like = new Like();
            like.id = (int)(l & 0x00000000ffffffffL);
            like.ts = (int)(l >> 32);
            return like;
        }).collect(Collectors.toList()) : new ArrayList<>();
        Set<Integer> setLike1 = new HashSet<>();
        setLike1.addAll(like1.stream().map(l -> l.id).collect(Collectors.toList()));
        Set<Integer> sharedLikes = new HashSet<>();
        for (Like l: like2) {
            if (setLike1.contains(l.id)) {
                sharedLikes.add(l.id);
            }
        }
        if (sharedLikes.isEmpty()) {
            return 0;
        }
        Map<Integer, List<Like>> likeMap1 = new HashMap<>();
        for (Like l: like1) {
            if (sharedLikes.contains(l.id)) {
                if (!likeMap1.containsKey(l.id)) {
                    likeMap1.put(l.id, new ArrayList<>());
                }
                likeMap1.get(l.id).add(l);
            }
        }
        Map<Integer, List<Like>> likeMap2 = new HashMap<>();
        for (Like l: like2) {
            if (sharedLikes.contains(l.id)) {
                if (!likeMap2.containsKey(l.id)) {
                    likeMap2.put(l.id, new ArrayList<>());
                }
                likeMap2.get(l.id).add(l);
            }
        }
        double similarity = 0;
        for (Integer like: sharedLikes) {
            List<Like> l1 = likeMap1.get(like);
            List<Like> l2 = likeMap2.get(like);
            double t1 = l1.stream().mapToDouble(l -> l.ts).average().getAsDouble();
            double t2 = l2.stream().mapToDouble(l -> l.ts).average().getAsDouble();
            if (t1 == t2) {
                return Double.MAX_VALUE;
            } else {
                if (t1 == t2) {
                    similarity+=1;
                } else {
                    similarity += 1 / Math.abs(t1 - t2);
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
        if (!accountDTO.email.matches(EMAIL_REG)) {
            throw new BadRequest();
        }
        if (findById(accountDTO.id) != null) {
            throw new BadRequest();
        }
        if (emails.contains(accountDTO.email)) {
            throw new BadRequest();
        }
        if (accountDTO.phone != null && phones.contains(accountDTO.phone)) {
            throw new BadRequest();
        }

        this.load(accountConverter.convert(accountDTO));
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
        Account oldAcc = findById(accountDTO.id);
        if (oldAcc == null) {
            throw new NotFoundRequest();
        }
        if (accountDTO.email != null && !accountDTO.email.matches(EMAIL_REG)) {
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
            oldAcc.fname = accountDTO.fname;
        }
        if (accountDTO.sname != null) {
            oldAcc.sname = accountDTO.sname;
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
            oldAcc.interests = accountDTO.interests;
        }
        if (accountDTO.premiumStart != 0) {
            oldAcc.premiumStart = accountDTO.premiumStart;
            oldAcc.premiumFinish = accountDTO.premiumFinish;
        }
        if (accountDTO.likes != null) {
            oldAcc.likes = accountDTO.likes;
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
        for (LikeRequest likeRequest: likeRequests) {
            if (likeRequest.likee == -1 || likeRequest.liker == -1 || likeRequest.ts == -1 || findById(likeRequest.likee) == null || findById(likeRequest.liker) == null) {
                throw new BadRequest();
            }
        }

        for (LikeRequest likeRequest: likeRequests) {
            Account account = findById(likeRequest.liker);
            long like =0;
            like = (int)(0x0000ffff & likeRequest.likee);
            like = ((long)likeRequest.ts << 32 | like) ;
            long[] oldArray = account.likes;
            if (oldArray == null) {
                account.likes = new long[1];
                account.likes[0] = like;
            } else {
                account.likes = new long[oldArray.length + 1];
                System.arraycopy(oldArray, 0, account.likes, 0, oldArray.length);
                account.likes[oldArray.length] = like;
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

    public Account findById(int id) {
        return accountIdMap[id];
    }

    public void finishLoad() {
        indexHolder.init(this.accountList, size);
        System.gc();
    }


    private List<IndexScan> getAvailableIndexScan(List<Predicate<Account>> predicates) {
        List<IndexScan> indexScans = new ArrayList<>();
        Iterator<Predicate<Account>> iterator = predicates.iterator();
        while (iterator.hasNext()) {
            Predicate<Account> predicate = iterator.next();
            if (predicate instanceof CountryEqPredicate) {
                CountryEqPredicate countryEqPredicate = (CountryEqPredicate) predicate;
                indexScans.add(new CountryEqIndexScan(indexHolder, countryEqPredicate.getCounty()));
                iterator.remove();
            } else if (predicate instanceof CountryNullPredicate) {
                CountryNullPredicate countryNullPredicate = (CountryNullPredicate) predicate;
                indexScans.add(new CountryNullIndexScan(indexHolder, countryNullPredicate.getNill()));
                iterator.remove();
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
            } else if (predicate instanceof SexEqPredicate) {
                SexEqPredicate sexEqPredicate = (SexEqPredicate) predicate;
                indexScans.add(new SexEqIndexScan(indexHolder, sexEqPredicate.getSex()));
                iterator.remove();
            } else if (predicate instanceof StatusNEqPredicate) {
                StatusNEqPredicate statusNEqPredicate = (StatusNEqPredicate) predicate;
                indexScans.add(new StatusNotEqIndexScan(indexHolder, statusNEqPredicate.getStatus()));
                iterator.remove();
            } else if (predicate instanceof CityNullPredicate) {
                CityNullPredicate cityNullPredicate = (CityNullPredicate) predicate;
                indexScans.add(new CityNullIndexScan(indexHolder, cityNullPredicate.getNill()));
                iterator.remove();
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
                indexHolder.init(accountList, size);
                System.gc();
                System.out.println("End update indexes " + new Date());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static class IntegerHolder {
        public int count;
    }

    private static class Similarity {
        public Account account;
        public double similarity;
    }

    private static class Like  {
        public int ts;
        public int id;
    }

}
