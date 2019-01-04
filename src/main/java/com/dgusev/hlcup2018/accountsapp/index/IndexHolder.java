package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.predicate.JoinedYearPredicate;
import gnu.trove.impl.Constants;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
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
    public TIntObjectMap<int[]> birthYearIndex;
    public Map<String, int[]> emailDomainIndex;
    public TIntObjectMap<int[]> fnameIndex;
    public TIntObjectMap<int[]> snameIndex;
    public TObjectIntMap<String> emailIndex;
    public TObjectIntMap<String> phoneIndex;
    public Map<String, int[]> phoneCodeIndex;

    public int[] notNullCountry;
    public int[] nullCountry;
    public int[] notNullCity;
    public int[] nullCity;
    public int[] notNullFname;
    public int[] nullFname;
    public int[] notNullSname;
    public int[] nullSname;
    public int[] notNullPhone;
    public int[] nullPhone;
    public int[] notNullPremium;
    public int[] nullPremium;
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
                Map<String, List<Integer>> tmpEmailDomainIndex = new HashMap<>();
                List<Integer> tmpNotNullCountry = new ArrayList<>();
                List<Integer> tmpNullCountry = new ArrayList<>();
                List<Integer> tmpNotNullCity = new ArrayList<>();
                List<Integer> tmpNullCity = new ArrayList<>();
                List<Integer> tmpNotNullFname = new ArrayList<>();
                List<Integer> tmpNullFname = new ArrayList<>();
                Map<Byte, List<Integer>> tmpStatusIndex = new HashMap<>();
                for (int i = 0; i < 3; i++) {
                    tmpStatusIndex.put((byte) i, new ArrayList<>());
                }
                Map<Byte, List<Integer>> tmpInterestsIndex = new HashMap<>();
                Map<Integer, List<Integer>> tmpFnameIndex = new HashMap<>();
                List<Integer> tmpPremiumIndex = new ArrayList<>();
                emailIndex = new TObjectIntHashMap<>();
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                        tmpCountryIndex.computeIfAbsent(account.country, k -> new ArrayList<>()).add(account.id);
                        tmpNotNullCountry.add(account.id);
                    } else {
                        tmpNullCountry.add(account.id);
                    }
                    if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        tmpNotNullCity.add(account.id);
                    } else {
                        tmpNullCity.add(account.id);
                    }
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
                    if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        tmpNotNullFname.add(account.id);
                        if (!tmpFnameIndex.containsKey(account.fname)) {
                            tmpFnameIndex.put(account.fname, new ArrayList<>());
                        }
                        tmpFnameIndex.get(account.fname).add(account.id);
                    } else {
                        tmpNullFname.add(account.id);
                    }
                    if (account.email != null) {
                        emailIndex.put(account.email, account.id);
                    }
                }
                countryIndex = new TByteObjectHashMap<>();
                for (Map.Entry<Byte, List<Integer>> entry : tmpCountryIndex.entrySet()) {
                    countryIndex.put(entry.getKey(), entry.getValue().stream()
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
                notNullFname = tmpNotNullFname.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                nullFname = tmpNullFname.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                fnameIndex = new TIntObjectHashMap<>();
                for (Map.Entry<Integer, List<Integer>> entry : tmpFnameIndex.entrySet()) {
                    fnameIndex.put(entry.getKey(), entry.getValue().stream()
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
                System.out.println("Finish init IndexHolder " + new Date());
            }
        };
        Runnable task2 = new Runnable() {
            @Override
            public void run() {
                Map<Boolean, List<Integer>> tmpSexIndex = new HashMap<>();
                Map<String, List<Integer>> tmpPhoneCodeIndex = new HashMap<>();
                List<Integer> tmpNotNullSname = new ArrayList<>();
                List<Integer> tmpNullSname = new ArrayList<>();
                List<Integer> tmpNotNullPhone = new ArrayList<>();
                List<Integer> tmpNullPhone = new ArrayList<>();
                List<Integer> tmpNotNullPremium = new ArrayList<>();
                List<Integer> tmpNullPremium = new ArrayList<>();
                Map<Integer, List<Integer>> tmpBirthYearIndex = new HashMap<>();
                tmpSexIndex.put(true, new ArrayList<>());
                tmpSexIndex.put(false, new ArrayList<>());
                Map<Integer, List<Integer>> tmpCityIndex = new HashMap<>();
                Map<Integer, List<Integer>> tmpSnameIndex = new HashMap<>();
                Map<Integer, List<Integer>> tmpJoinedIndex = new HashMap<>();
                phoneIndex = new TObjectIntHashMap<>();
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        tmpCityIndex.computeIfAbsent(account.city, k -> new ArrayList<>()).add(account.id);
                    }
                    tmpSexIndex.get(account.sex).add(account.id);
                    int year = new Date(account.birth * 1000L).getYear() + 1900;
                    tmpBirthYearIndex.computeIfAbsent(year, k -> new ArrayList<>()).add(account.id);
                    if (account.joined != Integer.MIN_VALUE) {
                        int jyear = JoinedYearPredicate.calculateYear(account.joined);
                        if (!tmpJoinedIndex.containsKey(jyear)) {
                            tmpJoinedIndex.put(jyear, new ArrayList<>());
                        }
                        tmpJoinedIndex.get(jyear).add(account.id);
                    }
                    if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        tmpNotNullSname.add(account.id);
                        if (!tmpSnameIndex.containsKey(account.sname)) {
                            tmpSnameIndex.put(account.sname, new ArrayList<>());
                        }
                        tmpSnameIndex.get(account.sname).add(account.id);
                    } else {
                        tmpNullSname.add(account.id);
                    }
                    if (account.phone != null) {
                        phoneIndex.put(account.phone, account.id);
                        tmpNotNullPhone.add(account.id);
                        int open = account.phone.indexOf("(");
                        if (open != -1) {
                            int close = account.phone.indexOf(')', open + 1);
                            if (close != -1) {
                                String code = account.phone.substring(open + 1, close);
                                if (!tmpPhoneCodeIndex.containsKey(code)) {
                                    tmpPhoneCodeIndex.put(code, new ArrayList<>());
                                }
                                tmpPhoneCodeIndex.get(code).add(account.id);
                            }
                        }
                    } else {
                        tmpNullPhone.add(account.id);
                    }
                    if (account.premiumStart != 0) {
                        tmpNotNullPremium.add(account.id);
                    } else {
                        tmpNullPremium.add(account.id);
                    }
                }
                sexIndex = new TByteObjectHashMap<>();
                for (Map.Entry<Boolean, List<Integer>> entry : tmpSexIndex.entrySet()) {
                    sexIndex.put(entry.getKey() ? (byte) 1 : 0, entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                notNullSname = tmpNotNullSname.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                nullSname = tmpNullSname.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                notNullPhone = tmpNotNullPhone.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                nullPhone = tmpNullPhone.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                notNullPremium = tmpNotNullPremium.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                nullPremium = tmpNullPremium.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
                cityIndex = new TIntObjectHashMap<>();
                for (Map.Entry<Integer, List<Integer>> entry : tmpCityIndex.entrySet()) {
                    cityIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                snameIndex = new TIntObjectHashMap<>();
                for (Map.Entry<Integer, List<Integer>> entry : tmpSnameIndex.entrySet()) {
                    snameIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                birthYearIndex = new TIntObjectHashMap<>();
                for (Map.Entry<Integer, List<Integer>> entry : tmpBirthYearIndex.entrySet()) {
                    birthYearIndex.put(entry.getKey(), entry.getValue().stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                }
                phoneCodeIndex = new HashMap<>();
                for (Map.Entry<String, List<Integer>> entry : tmpPhoneCodeIndex.entrySet()) {
                    phoneCodeIndex.put(entry.getKey(), entry.getValue().stream()
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
        Runnable task3 = new Runnable() {

            @Override
            public void run() {
                likesIndex = null;
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
                            set.add(id);
                        }
                    }
                }
                likesIndex = new TIntObjectHashMap<>();

                for (int key : tmpLikesIndex.keys()) {
                    likesIndex.put(key, tmpLikesIndex.get(key).toArray());
                    tmpLikesIndex.put(key, null);
                }
            }
        };
        executorService.submit(task1).get();
        System.gc();
        executorService.submit(task2).get();
        System.gc();
        System.out.println("Before start task3");
        System.out.println("Start task3");
        executorService.submit(task3).get();
        System.gc();
    }

}
