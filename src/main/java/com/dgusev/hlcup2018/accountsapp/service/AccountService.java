package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.*;
import com.dgusev.hlcup2018.accountsapp.predicate.SexEqPredicate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class AccountService {
    private static final Set<String> ALLOWED_SEX = new HashSet<>(Arrays.asList("m", "f"));
    private static final Set<String> ALLOWED_STATUS = new HashSet<>(Arrays.asList("свободны", "всё сложно","заняты"));
    private static final String EMAIL_REG = "[0-9a-zA-z]+@[0-9a-zA-z]+\\.[0-9a-zA-z]+";

    @Autowired
    private NowProvider nowProvider;

    private List<AccountDTO> accountDTOList = new ArrayList<>();

    public List<AccountDTO> filter(List<Predicate<AccountDTO>> predicates, int limit) {
        Predicate<AccountDTO> accountPredicate = null;
        if (predicates.isEmpty()) {
            accountPredicate = foo -> true;
        } else {
            accountPredicate = predicates.get(0);
            for (int i = 1; i < predicates.size(); i++) {
                accountPredicate = accountPredicate.and(predicates.get(i));
            }
        }
        return accountDTOList.stream().filter(accountPredicate).sorted(Comparator.comparingInt(a -> ((AccountDTO)a).id).reversed()).limit(limit).collect(Collectors.toList());
    }

    public List<Group> group(List<String> keys, List<Predicate<AccountDTO>> predicates, int order, int limit) {
        Predicate<AccountDTO> accountPredicate = null;
        if (predicates.isEmpty()) {
            accountPredicate = foo -> true;
        } else {
            accountPredicate = predicates.get(0);
            for (int i = 1; i < predicates.size(); i++) {
                accountPredicate = accountPredicate.and(predicates.get(i));
            }
        }
        HashMap<List<String>, Integer> groupMap = new HashMap<>();
        for (AccountDTO accountDTO : accountDTOList) {
            if (accountPredicate.test(accountDTO)) {
                List<String> group = new ArrayList<>();
                for (String key: keys) {
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
                    if (accountDTO.interests == null || accountDTO.interests.size() == 0) {
                        group.add(null);
                        incrementGroup(groupMap, group);
                    } else {
                        for (String interes: accountDTO.interests) {
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
        }).sorted((g1,g2) -> {
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
            if (g1.get(i) == null) {
                return -1;
            } else if (g2.get(i) == null) {
                return 1;
            } else {
                int cc = g1.get(i).compareTo(g2.get(i));
                if (cc != 0) {
                    return cc;
                }
            }
        }
        return 0;
    }


    public List<AccountDTO> recommend(Integer id, List<Predicate<AccountDTO>> predicates, int limit) {
        AccountDTO accountDTO = accountDTOList.stream().filter(a -> a.id == id).findFirst().orElseThrow(NotFoundRequest::new);
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
        Set<String> interests = new HashSet<>();
        if (accountDTO.interests != null) {
            interests.addAll(accountDTO.interests);
        }
        return accountDTOList.stream().filter(accountPredicate).sorted( (a1, a2) -> {
            if (isPremium(a1) && !isPremium(a2)) {
                return -1;
            } else if (!isPremium(a1) && isPremium(a2)) {
                return 1;
            }
            int status1 = getStatusNumber(a1.status);
            int status2 = getStatusNumber(a1.status);
            int cc1 = Integer.compare(status1, status2);
            if (cc1 != 0) {
                return cc1;
            }
            int int1 = interestsMatched(interests, a1.interests);
            int int2 = interestsMatched(interests, a2.interests);
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
        }).limit(limit).collect(Collectors.toList());
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

    private int interestsMatched(Set<String> myInterests, List<String> othersInterests) {
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


    public List<AccountDTO> suggest(Integer id, List<Predicate<AccountDTO>> predicates, int limit) {
        AccountDTO accountDTO = accountDTOList.stream().filter(a -> a.id == id).findFirst().orElseThrow(NotFoundRequest::new);
        predicates.add(new SexEqPredicate(accountDTO.sex));
        predicates.add(a -> a.id != id);
        Set<Integer> likes = new HashSet<>();
        if (accountDTO.likes != null && accountDTO.likes.isEmpty()) {
            likes.addAll(accountDTO.likes.stream().map(l -> l.id).collect(Collectors.toList()));
        }
        Predicate<AccountDTO> accountPredicate = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            accountPredicate = accountPredicate.and(predicates.get(i));
        }


         return accountDTOList.stream().filter(accountPredicate).sorted( (a1, a2) -> {
            double s1 = getSimilarity(accountDTO, a1);
            double s2 = getSimilarity(accountDTO, a2);
            return Double.compare(s1, s2);
        }).flatMap(a -> a.likes != null ? a.likes.stream().sorted(Comparator.comparingInt(l -> l.id)) : new ArrayList<AccountDTO.Like>().stream())
                 .filter(l -> !likes.contains(l.id)).limit(limit).map(l-> {
                     for (AccountDTO ac: accountDTOList) {
                         if (ac.id == l.id) {
                             return ac;
                         }
                     }
                     return null;
         }).collect(Collectors.toList());
    }

    private double getSimilarity(AccountDTO a1, AccountDTO a2) {
        List<AccountDTO.Like> like1 = a1.likes != null ? a1.likes : new ArrayList<AccountDTO.Like>();
        List<AccountDTO.Like> like2 = a1.likes != null ? a1.likes : new ArrayList<AccountDTO.Like>();
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

    public void load(AccountDTO accountDTO) {
        accountDTOList.add(accountDTO);
    }


    public void add(AccountDTO accountDTO) {
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
        for (AccountDTO acc: accountDTOList) {
            if (acc.id == accountDTO.id) {
                throw new BadRequest();
            }
            if (acc.email.equals(accountDTO.email)) {
                throw new BadRequest();
            }
            if (accountDTO.phone != null && acc.phone != null && accountDTO.phone.equals(acc.phone)) {
                throw new BadRequest();
            }
        }
        accountDTOList.add(accountDTO);
    }

    public void update(AccountDTO accountDTO) {
        if (accountDTO.sex != null && !ALLOWED_SEX.contains(accountDTO.sex)) {
            throw new BadRequest();
        }
        if (accountDTO.status != null && !ALLOWED_STATUS.contains(accountDTO.status)) {
            throw new BadRequest();
        }
        AccountDTO oldAcc = null;
        for (AccountDTO acc: accountDTOList) {
            if (acc.id == accountDTO.id) {
                oldAcc = acc;
                break;
            }
        }
        if (oldAcc == null) {
            throw new NotFoundRequest();
        }
        if (accountDTO.email != null && !accountDTO.email.matches(EMAIL_REG)) {
            throw new BadRequest();
        }
        for (AccountDTO acc: accountDTOList) {
            if (acc.id != accountDTO.id) {
                if (acc.email.equals(accountDTO.email)) {
                    throw new BadRequest();
                }
                if (accountDTO.phone != null && acc.phone != null && accountDTO.phone.equals(acc.phone)) {
                    throw new BadRequest();
                }
            }
        }
        if (accountDTO.email != null) {
            oldAcc.email = accountDTO.email;
        }
        if (accountDTO.fname != null) {
            oldAcc.fname = accountDTO.fname;
        }
        if (accountDTO.sname != null) {
            oldAcc.sname = accountDTO.sname;
        }
        if (accountDTO.phone != null) {
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
    }


    public void like(List<LikeRequest> likeRequests) {
        for (LikeRequest likeRequest: likeRequests) {
            if (likeRequest.likee == -1 || likeRequest.liker == -1 || likeRequest.ts == -1 || findById(likeRequest.likee) == null || findById(likeRequest.liker) == null) {
                throw new BadRequest();
            }
        }

        for (LikeRequest likeRequest: likeRequests) {
            AccountDTO accountDTO = findById(likeRequest.liker);
            if (accountDTO.likes == null) {
                accountDTO.likes = new ArrayList<>();
            }
            AccountDTO.Like like = new AccountDTO.Like();
            like.id = likeRequest.likee;
            like.ts = likeRequest.ts;
            accountDTO.likes.add(like);
        }

    }

    private AccountDTO findById(int id) {
        for (AccountDTO accountDTO : accountDTOList) {
            if (accountDTO.id == id) {
                return accountDTO;
            }
        }
        return null;
    }

}
