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
    public  static final int MAX_ID = 520000;
    private static final Set<String> ALLOWED_SEX = new HashSet<>(Arrays.asList("m", "f"));
    private static final Set<String> ALLOWED_STATUS = new HashSet<>(Arrays.asList("свободны", "всё сложно","заняты"));
    private static final String EMAIL_REG = "[0-9a-zA-z]+@[0-9a-zA-z]+\\.[0-9a-zA-z]+";


    @Autowired
    private NowProvider nowProvider;

    @Autowired
    private Dictionary dictionary;

    @Autowired
    private IndexHolder indexHolder;

    private AccountDTO[] accountDTOList = new AccountDTO[MAX_ID];
    private volatile int size;
    private AccountDTO[] accountIdMap = new AccountDTO[MAX_ID];

    private Set<String> emails = new HashSet<>();
    private Set<String> phones = new HashSet<>();

    public static volatile long LAST_UPDATE_TIMESTAMP;

    public List<AccountDTO> filter(List<Predicate<AccountDTO>> predicates, int limit) {
        if (limit == 0) {
            return new ArrayList<>();
        }
        List<IndexScan> indexScans = getAvailableIndexScan(predicates);
        if (!indexScans.isEmpty()) {
            Predicate<AccountDTO> accountPredicate = andPredicates(predicates);
            List<AccountDTO> result = new ArrayList<>();
            int count = 0;
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }

                AccountDTO accountDTO = findById(next);
                if (accountPredicate.test(accountDTO)) {
                    result.add(accountDTO);
                    count++;
                    if (count == limit) {
                        break;
                    }
                }
            }
            return result;
        } else {
            Predicate<AccountDTO> accountPredicate = andPredicates(predicates);
            return filterSeqScan(accountPredicate, limit);
        }
    }

    private List<AccountDTO> filterSeqScan(Predicate<AccountDTO> predicate, int limit) {

        List<AccountDTO> result = new ArrayList<>();
        int count = 0;
        if (limit == 0) {
            return new ArrayList<>();
        }
        for (int i = 0; i< size; i++) {
            AccountDTO accountDTO = accountDTOList[i];
            if (predicate.test(accountDTO)) {
                result.add(accountDTO);
                count++;
                if (count == limit) {
                    break;
                }
            }
        }
        return result;
    }

    public List<Group> group(List<String> keys, List<Predicate<AccountDTO>> predicates, int order, int limit) {
        List<IndexScan> indexScans = getAvailableIndexScan(predicates);

        if (!indexScans.isEmpty()) {
            Predicate<AccountDTO> accountPredicate = andPredicates(predicates);
            IndexScan indexScan = new CompositeIndexScan(indexScans);
            HashMap<List<String>, Integer> groupMap = new HashMap<>();
            while (true) {
                int next = indexScan.getNext();
                if (next == -1) {
                    break;
                }
                AccountDTO accountDTO = findById(next);
                if (accountPredicate.test(accountDTO)) {
                    List<String> group = new ArrayList<>();
                    for (String key : keys) {
                        if (key.equals("sex")) {
                            group.add(accountDTO.sex);
                        } else if (key.equals("status")) {
                            group.add(accountDTO.status);
                        } else if (key.equals("interests")) {

                        } else if (key.equals("country")) {
                            group.add(accountDTO.country);
                        } else if (key.equals("city")) {
                            group.add(accountDTO.city);
                        } else {
                            throw new BadRequest();
                        }
                    }
                    if (keys.contains("interests")) {
                        if (accountDTO.interests == null || accountDTO.interests.length == 0) {
                            //group.add(null);
                            //incrementGroup(groupMap, group);
                        } else {
                            for (String interes : accountDTO.interests) {
                                List<String> newGroup = new ArrayList<>(group);
                                newGroup.add(interes);
                                incrementGroup(groupMap, newGroup);
                            }
                        }
                    } else {
                        incrementGroup(groupMap, group);
                    }

                }
            }
            return groupMap.entrySet().stream().map(e -> {
                Group group = new Group();
                group.count = e.getValue();
                group.values = e.getKey();
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
        } else {
            Predicate<AccountDTO> accountPredicate = andPredicates(predicates);
            HashMap<List<String>, Integer> groupMap = new HashMap<>();

            for (int i = 0; i< size; i++) {
                AccountDTO accountDTO = accountDTOList[i];
                if (accountPredicate.test(accountDTO)) {
                    List<String> group = new ArrayList<>();
                    for (String key : keys) {
                        if (key.equals("sex")) {
                            group.add(accountDTO.sex);
                        } else if (key.equals("status")) {
                            group.add(accountDTO.status);
                        } else if (key.equals("interests")) {

                        } else if (key.equals("country")) {
                            group.add(accountDTO.country);
                        } else if (key.equals("city")) {
                            group.add(accountDTO.city);
                        } else {
                            throw new BadRequest();
                        }
                    }
                    if (keys.contains("interests")) {
                        if (accountDTO.interests == null || accountDTO.interests.length == 0) {
                          //  group.add(null);
                         //   incrementGroup(groupMap, group);
                        } else {
                            for (String interes : accountDTO.interests) {
                                List<String> newGroup = new ArrayList<>(group);
                                newGroup.add(interes);
                                incrementGroup(groupMap, newGroup);
                            }
                        }

                    } else {
                        incrementGroup(groupMap, group);
                    }

                }
            }
            return groupMap.entrySet().stream().map(e -> {
                Group group = new Group();
                group.count = e.getValue();
                group.values = e.getKey();
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
    }

    private void incrementGroup(HashMap<List<String>, Integer> groupMap, List<String> group) {
        if (!groupMap.containsKey(group)) {
            groupMap.put(group, 1);
        } else {
            int count =  groupMap.get(group);
            groupMap.put(group, count + 1);
        }
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


    public List<AccountDTO> recommend(int id, List<Predicate<AccountDTO>> predicates, int limit) {
        AccountDTO accountDTO = findById(id);
        if (accountDTO == null) {
            throw new NotFoundRequest();
        }
        if (accountDTO.sex.equals("m")) {
            predicates.add(new SexEqPredicate("f"));
        } else {
            predicates.add(new SexEqPredicate("m"));
        }
        predicates.add(a -> a.id != id);
        Predicate<AccountDTO> accountPredicate = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            accountPredicate = accountPredicate.and(predicates.get(i));
        }
        accountPredicate = accountPredicate.and(a -> {
           if (accountDTO.interests == null || a.interests == null || accountDTO.interests.length == 0 || a.interests.length == 0) {
               return false;
           }
           if (interestsMatched(Arrays.asList(accountDTO.interests), Arrays.asList(a.interests)) == 0) {
               return false;
           }
           return true;

        });
        Set<String> interests = new HashSet<>();
        if (accountDTO.interests != null) {
            interests.addAll(Arrays.asList(accountDTO.interests));
        }
        List<AccountDTO> result = new ArrayList<>();
        for (int i = 0; i< size; i++) {
            AccountDTO account = accountDTOList[i];
            if (accountPredicate.test(account)) {
                result.add(account);
            }
        }
        result.sort((a1, a2) -> {
            if (isPremium(a1) && !isPremium(a2)) {
                return -1;
            } else if (!isPremium(a1) && isPremium(a2)) {
                return 1;
            }
            int status1 = getStatusNumber(a1.status);
            int status2 = getStatusNumber(a2.status);
            int cc1 = Integer.compare(status1, status2);
            if (cc1 != 0) {
                return cc1;
            }
            int int1 = interestsMatched(interests, Arrays.asList(a1.interests != null ? a1.interests : new String[0]));
            int int2 = interestsMatched(interests, Arrays.asList(a2.interests != null ? a2.interests : new String[0]));
            int cc2 = Integer.compare(int1, int2);
            if (cc2 != 0) {
                return -cc2;
            }
            int bd1 = Math.abs(a1.birth - accountDTO.birth);
            int bd2 = Math.abs(a2.birth - accountDTO.birth);
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



    private int getStatusNumber(String status) {
        if (status.equals("свободны")) {
            return 0;
        } else if (status.equals("всё сложно")) {
            return 1;
        } else {
            return 2;
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

    private boolean isPremium(AccountDTO accountDTO) {
        return accountDTO.premiumStart != 0 && accountDTO.premiumStart < nowProvider.getNow() && (accountDTO.premiumFinish > nowProvider.getNow() || accountDTO.premiumFinish == 0);
    }


    public List<AccountDTO> suggest(int id, List<Predicate<AccountDTO>> predicates, int limit) {
        AccountDTO accountDTO = findById(id);
        if (accountDTO == null) {
            throw new NotFoundRequest();
        }
        if (true) {
            return Collections.EMPTY_LIST;
        }
        predicates.add(new SexEqPredicate(accountDTO.sex));
        predicates.add(a -> a.id != id);
        Set<Integer> likes = new HashSet<>();
        if (accountDTO.likes != null && accountDTO.likes.length != 0) {
            likes.addAll(Arrays.stream(accountDTO.likes).map(l -> l.id).collect(Collectors.toList()));
        }
        Predicate<AccountDTO> accountPredicate = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            accountPredicate = accountPredicate.and(predicates.get(i));
        }

         return Arrays.stream(accountDTOList,0 , size).filter(accountPredicate).sorted( (a1, a2) -> {
            double s1 = getSimilarity(accountDTO, a1);
            double s2 = getSimilarity(accountDTO, a2);
            return Double.compare(s1, s2);
        }).flatMap(a -> a.likes != null ? Arrays.stream(a.likes).sorted(Comparator.comparingInt((AccountDTO.Like l) -> l.id).reversed()) : new ArrayList<AccountDTO.Like>().stream())
                 .filter(l -> !likes.contains(l.id)).limit(limit).map(l-> accountIdMap[l.id]).collect(Collectors.toList());
    }

    private double getSimilarity(AccountDTO a1, AccountDTO a2) {
        List<AccountDTO.Like> like1 = a1.likes != null ?Arrays.asList( a1.likes) : new ArrayList<AccountDTO.Like>();
        List<AccountDTO.Like> like2 = a2.likes != null ? Arrays.asList(a2.likes) : new ArrayList<AccountDTO.Like>();
        Set<Integer> setLike1 = new HashSet<>();
        setLike1.addAll(like1.stream().map(l -> l.id).collect(Collectors.toList()));
        Set<Integer> sharedLikes = new HashSet<>();
        for (AccountDTO.Like l: like2) {
            if (setLike1.contains(l.id)) {
                sharedLikes.add(l.id);
            }
        }
        if (sharedLikes.isEmpty()) {
            return 0;
        }
        Map<Integer, List<AccountDTO.Like>> likeMap1 = new HashMap<>();
        for (AccountDTO.Like l: like1) {
            if (sharedLikes.contains(l.id)) {
                if (!likeMap1.containsKey(l.id)) {
                    likeMap1.put(l.id, new ArrayList<>());
                }
                likeMap1.get(l.id).add(l);
            }
        }
        Map<Integer, List<AccountDTO.Like>> likeMap2 = new HashMap<>();
        for (AccountDTO.Like l: like2) {
            if (sharedLikes.contains(l.id)) {
                if (!likeMap2.containsKey(l.id)) {
                    likeMap2.put(l.id, new ArrayList<>());
                }
                likeMap2.get(l.id).add(l);
            }
        }
        double similarity = 0;
        for (Integer like: sharedLikes) {
            List<AccountDTO.Like> l1 = likeMap1.get(like);
            List<AccountDTO.Like> l2 = likeMap2.get(like);
            double t1 = l1.stream().mapToDouble(l -> l.ts).average().getAsDouble();
            double t2 = l2.stream().mapToDouble(l -> l.ts).average().getAsDouble();
            if (t1 == t2) {
                return Double.MAX_VALUE;
            } else {
                similarity += 1 / Math.abs(t1 - t2);
            }
        }
        return similarity;
    }

    public synchronized void load(AccountDTO accountDTO) {
        if (size == 0) {
            accountDTOList[size] = accountDTO;
            size++;
        } else {
            for (int i = 0; i < size; i++) {
                if (accountDTO.id > accountDTOList[i].id) {
                    System.arraycopy(accountDTOList, i, accountDTOList, i+1, size - i);
                    accountDTOList[i] = accountDTO;
                    size++;
                    break;
                }
            }
        }
        accountIdMap[accountDTO.id] =  accountDTO;
        emails.add(accountDTO.email);
        if (accountDTO.phone != null) {
            phones.add(accountDTO.phone);
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

        this.load(accountDTO);
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
        AccountDTO oldAcc = findById(accountDTO.id);
        if (oldAcc == null) {
            throw new NotFoundRequest();
        }
        if (accountDTO.email != null && !accountDTO.email.matches(EMAIL_REG)) {
            throw new BadRequest();
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
        }
        if (accountDTO.sex != null) {
            oldAcc.sex = accountDTO.sex;
        }
        if (accountDTO.birth != Integer.MIN_VALUE) {
            oldAcc.birth = accountDTO.birth;
        }
        if (accountDTO.country != null) {
            oldAcc.country = accountDTO.country;
        }
        if (accountDTO.city != null) {
            oldAcc.city = accountDTO.city;
        }
        if (accountDTO.joined != Integer.MIN_VALUE) {
            oldAcc.joined = accountDTO.joined;
        }
        if (accountDTO.status != null) {
            oldAcc.status = accountDTO.status;
        }
        if (accountDTO.interests != null) {
            oldAcc.interests = accountDTO.interests;
        }
        if (accountDTO.premiumStart != Integer.MIN_VALUE) {
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
            AccountDTO accountDTO = findById(likeRequest.liker);
            AccountDTO.Like like = new AccountDTO.Like();
            like.id = likeRequest.likee;
            like.ts = likeRequest.ts;
            AccountDTO.Like[] oldArray = accountDTO.likes;
            if (oldArray == null) {
                accountDTO.likes = new AccountDTO.Like[1];
                accountDTO.likes[0] = like;
            } else {
                accountDTO.likes = new AccountDTO.Like[oldArray.length + 1];
                System.arraycopy(oldArray, 0, accountDTO.likes, 0, oldArray.length);
                accountDTO.likes[oldArray.length] = like;
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

    public AccountDTO findById(int id) {
        return accountIdMap[id];
    }

    public void finishLoad() {
        indexHolder.init(this.accountDTOList, size);
        System.gc();
    }


    private List<IndexScan> getAvailableIndexScan(List<Predicate<AccountDTO>> predicates) {
        List<IndexScan> indexScans = new ArrayList<>();
        Iterator<Predicate<AccountDTO>> iterator = predicates.iterator();
        while (iterator.hasNext()) {
            Predicate<AccountDTO> predicate = iterator.next();
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

    private Predicate<AccountDTO> andPredicates(List<Predicate<AccountDTO>> predicates) {
        Predicate<AccountDTO> accountPredicate = null;
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
            while (LAST_UPDATE_TIMESTAMP == 0 || System.currentTimeMillis() - LAST_UPDATE_TIMESTAMP  < 2000){
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            try {
                System.out.println("Start update indexes " + new Date());
                indexHolder.init(accountDTOList, size);
                System.gc();
                System.out.println("End update indexes " + new Date());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
