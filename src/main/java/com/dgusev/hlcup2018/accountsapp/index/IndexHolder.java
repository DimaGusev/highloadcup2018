package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.predicate.JoinedYearPredicate;
import gnu.trove.impl.Constants;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class IndexHolder {

    public TByteObjectMap<int[]> countryIndex;
    public TByteObjectMap<int[]> sexIndex;
    public TByteObjectMap<int[]> statusIndex;
    public TByteObjectMap<int[]> interestsIndex;
    public TIntObjectMap<int[]> cityIndex;
    public TIntObjectMap<int[]> joinedIndex;
    public Map<Integer, int[]> birthYearIndex;
    public Map<String, int[]> emailDomainIndex;
    public int[] notNullCountry;
    public int[] nullCountry;
    public int[] notNullCity;
    public int[] nullCity;
    public int[] premiumIndex;
    public TIntObjectMap<int[]> likesIndex;

    @Autowired
    private NowProvider nowProvider;

    public synchronized void init(Account[] accountDTOList, int size) throws ExecutionException, InterruptedException {
        System.out.println("Start init IndexHolder " + new Date());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        int now = nowProvider.getNow();
        Runnable task1 = new Runnable() {
            @Override
            public void run() {
                Map<Byte, List<Integer>> tmpCountryIndex = new HashMap<>();
                Map<Boolean, List<Integer>> tmpSexIndex = new HashMap<>();
                Map<String, List<Integer>> tmpEmailDomainIndex = new HashMap<>();
                List<Integer> tmpNotNullCountry = new ArrayList<>();
                List<Integer> tmpNullCountry = new ArrayList<>();
                List<Integer> tmpNotNullCity = new ArrayList<>();
                List<Integer> tmpNullCity = new ArrayList<>();
                Map<Byte, List<Integer>> tmpStatusIndex = new HashMap<>();
                Map<Integer, List<Integer>> tmpBirthYearIndex = new HashMap<>();
                tmpSexIndex.put(true, new ArrayList<>());
                tmpSexIndex.put(false, new ArrayList<>());
                for (int i = 0; i < 3; i++) {
                    tmpStatusIndex.put((byte) i, new ArrayList<>());
                }
                Map<Byte, List<Integer>> tmpInterestsIndex = new HashMap<>();
                Map<Integer, List<Integer>> tmpCityIndex = new HashMap<>();
                List<Integer> tmpPremiumIndex = new ArrayList<>();
                Map<Integer, List<Integer>> tmpJoinedIndex = new HashMap<>();
                System.out.println("Start iteration " + new Date());


                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                        tmpCountryIndex.computeIfAbsent(account.country, k -> new ArrayList<>()).add(account.id);
                        tmpNotNullCountry.add(account.id);
                    } else {
                        tmpNullCountry.add(account.id);
                    }
                    if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        tmpCityIndex.computeIfAbsent(account.city, k -> new ArrayList<>()).add(account.id);
                        tmpNotNullCity.add(account.id);
                    } else {
                        tmpNullCity.add(account.id);
                    }
                    tmpSexIndex.get(account.sex).add(account.id);
                    tmpStatusIndex.get(account.status).add(account.id);

                    if (account.interests != null) {
                        for (byte interes : account.interests) {
                            tmpInterestsIndex.computeIfAbsent(interes, k -> new ArrayList<>()).add(account.id);
                        }
                    }
                    int at = account.email.lastIndexOf('@');
                    String domain = account.email.substring(at + 1);
                    tmpEmailDomainIndex.computeIfAbsent(domain, k -> new ArrayList<>()).add(account.id);
                    if (account.premiumStart != 0 && account.premiumStart <= now && (account.premiumFinish == 0 || account.premiumFinish > now)) {
                        tmpPremiumIndex.add(account.id);
                    }
                    int year = new Date(account.birth * 1000L).getYear() + 1900;
                    tmpBirthYearIndex.computeIfAbsent(year, k -> new ArrayList<>()).add(account.id);
                    if (account.joined != Integer.MIN_VALUE) {
                        int jyear = JoinedYearPredicate.calculateYear(account.joined);
                        if (!tmpJoinedIndex.containsKey(jyear)) {
                            tmpJoinedIndex.put(jyear, new ArrayList<>());
                        }
                        tmpJoinedIndex.get(jyear).add(account.id);
                    }
                }
                System.out.println("End iteration " + new Date());
                countryIndex = new TByteObjectHashMap<>();
                for (Map.Entry<Byte, List<Integer>> entry : tmpCountryIndex.entrySet()) {
                    countryIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                sexIndex = new TByteObjectHashMap<>();
                for (Map.Entry<Boolean, List<Integer>> entry : tmpSexIndex.entrySet()) {
                    sexIndex.put(entry.getKey() ? (byte) 1 : 0, entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                statusIndex = new TByteObjectHashMap<>();
                for (Map.Entry<Byte, List<Integer>> entry : tmpStatusIndex.entrySet()) {
                    statusIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                interestsIndex = new TByteObjectHashMap<>();
                for (Map.Entry<Byte, List<Integer>> entry : tmpInterestsIndex.entrySet()) {
                    interestsIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                notNullCountry = tmpNotNullCountry.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                nullCountry = tmpNullCountry.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                notNullCity = tmpNotNullCity.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                nullCity = tmpNullCity.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                cityIndex = new TIntObjectHashMap<>();
                for (Map.Entry<Integer, List<Integer>> entry : tmpCityIndex.entrySet()) {
                    cityIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                birthYearIndex = new HashMap<>();
                for (Map.Entry<Integer, List<Integer>> entry : tmpBirthYearIndex.entrySet()) {
                    birthYearIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                premiumIndex = tmpPremiumIndex.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                emailDomainIndex = new HashMap<>();
                for (Map.Entry<String, List<Integer>> entry : tmpEmailDomainIndex.entrySet()) {
                    emailDomainIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                joinedIndex = new TIntObjectHashMap<>();
                for (Map.Entry<Integer, List<Integer>> entry : tmpJoinedIndex.entrySet()) {
                    joinedIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                System.out.println("Finish init IndexHolder " + new Date());
            }
        };
        Runnable task2 = new Runnable() {

            @Override
            public void run() {
                TIntObjectMap<TIntArrayList> tmpLikesIndex = new TIntObjectHashMap<>();
                Set<Integer> set = new HashSet<>();
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.likes != null && account.likes.length != 0) {
                        set.clear();
                        for (long like : account.likes) {
                            int id = (int) (like >> 32);
                            if (set.contains(id)) {
                                continue;
                            }
                            if (!tmpLikesIndex.containsKey(id)) {
                                tmpLikesIndex.put(id, new TIntArrayList());
                            }
                            tmpLikesIndex.get(id).add(account.id);
                            set.add(account.id);
                        }
                    }
                }
                likesIndex = new TIntObjectHashMap<>();

                for (int key : tmpLikesIndex.keys()) {
                    likesIndex.put(key, tmpLikesIndex.get(key).toArray());
                }
            }
        };
        System.out.println("Start task1 " + new Date());
        executorService.submit(task1).get();
        System.out.println("Finish task1 " + new Date());
        System.gc();
        System.out.println("Start task2 " + new Date());
        executorService.submit(task2).get();
        System.out.println("Finish task2 " + new Date());
        System.gc();
    }

}
