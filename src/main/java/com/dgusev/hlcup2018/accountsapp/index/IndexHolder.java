package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.predicate.BirthYearPredicate;
import com.dgusev.hlcup2018.accountsapp.predicate.JoinedYearPredicate;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import gnu.trove.impl.Constants;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.*;
import gnu.trove.map.hash.*;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class IndexHolder {

    private static TLongObjectMap<String> domainsMap = new TLongObjectHashMap<>();
    private static TLongObjectMap<String> phoneCodeMap = new TLongObjectHashMap<>();


    private static int[] LIKE_TMP_ARRAY = new int[AccountService.MAX_ID];

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
    public int[][] likesIndex;

    @Autowired
    private NowProvider nowProvider;

    public synchronized void init(Account[] accountDTOList, int size, TIntObjectMap<TIntList> tIntListTIntObjectMap) throws ExecutionException, InterruptedException {
        System.out.println("Start init IndexHolder " + new Date());
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        int now = nowProvider.getNow();
        Callable<Boolean> task1 = new Callable<Boolean>() {
            @Override
            public Boolean call() {
                emailIndex = null;
                countryIndex = null;
                statusIndex = null;
                interestsIndex = null;
                notNullCountry = null;
                nullCountry = null;
                notNullCity = null;
                nullCity = null;
                notNullFname = null;
                nullFname = null;
                fnameIndex = null;
                premiumIndex = null;
                emailDomainIndex = null;
                TByteIntMap tmpCountryIndex = new TByteIntHashMap();
                TObjectIntMap<String> tmpEmailDomainIndex = new TObjectIntHashMap<>();
                int notNullCountryCounter = 0;
                int nullCountryCounter = 0;
                int notNullCityCounter = 0;
                int nullCityCounter = 0;
                int notNullFnameCounter = 0;
                int nullFnameCounter = 0;
                int premiumCounter = 0;
                TByteIntMap tmpStatusIndex = new TByteIntHashMap();
                for (int i = 0; i < 3; i++) {
                    tmpStatusIndex.put((byte) i, 0);
                }
                TByteIntMap tmpInterestsIndex = new TByteIntHashMap();
                TIntIntMap tmpFnameIndex = new TIntIntHashMap();
                emailIndex = new TObjectIntHashMap<>();
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                        int count = tmpCountryIndex.get(account.country);
                        if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                            tmpCountryIndex.put(account.country, 1);
                        } else {
                            tmpCountryIndex.put(account.country, count + 1);
                        }
                        notNullCountryCounter++;
                    } else {
                        nullCountryCounter++;
                    }
                    if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        notNullCityCounter++;
                    } else {
                        nullCityCounter++;
                    }
                    tmpStatusIndex.put(account.status, tmpStatusIndex.get(account.status) + 1);

                    if (account.interests != null) {
                        for (byte interes : account.interests) {
                            int count = tmpInterestsIndex.get(interes);
                            if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                tmpInterestsIndex.put(interes, 1);
                            } else {
                                tmpInterestsIndex.put(interes, count + 1);
                            }
                        }
                    }
                    int at = account.email.lastIndexOf('@');
                    long hash = calculateHash(account.email, at + 1 , account.email.length());
                    String domain = domainsMap.get(hash);
                    if (domain == null) {
                        domain = account.email.substring(at + 1);
                        domainsMap.put(hash, domain);
                    }
                    if (!tmpEmailDomainIndex.containsKey(domain)) {
                        tmpEmailDomainIndex.put(domain,  1);
                    } else  {
                        tmpEmailDomainIndex.put(domain,  tmpEmailDomainIndex.get(domain) + 1);
                    }
                    if (account.premium) {
                        premiumCounter++;
                    }
                    if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        notNullFnameCounter++;
                        int count = tmpFnameIndex.get(account.fname);
                        if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                            tmpFnameIndex.put(account.fname, 1);
                        } else {
                            tmpFnameIndex.put(account.fname, count + 1);
                        }
                    } else {
                        nullFnameCounter++;
                    }
                    if (account.email != null) {
                        emailIndex.put(account.email, account.id);
                    }
                }

                notNullCountry = new int[notNullCountryCounter];
                nullCountry = new int[nullCountryCounter];
                notNullCity = new int[notNullCityCounter];
                nullCity = new int[nullCityCounter];
                notNullFname = new int[notNullFnameCounter];
                nullFname = new int[nullFnameCounter];
                premiumIndex = new int[premiumCounter];
                notNullCountryCounter = 0;
                nullCountryCounter = 0;
                notNullCityCounter = 0;
                nullCityCounter = 0;
                notNullFnameCounter = 0;
                nullFnameCounter = 0;
                premiumCounter = 0;
                countryIndex = new TByteObjectHashMap<>();
                for (byte entry : tmpCountryIndex.keys()) {
                    countryIndex.put(entry, new int[tmpCountryIndex.get(entry)]);
                    tmpCountryIndex.put(entry, 0);
                }
                statusIndex = new TByteObjectHashMap<>();
                for (byte entry : tmpStatusIndex.keys()) {
                    statusIndex.put(entry, new int[tmpStatusIndex.get(entry)]);
                    tmpStatusIndex.put(entry, 0);
                }
                interestsIndex = new TByteObjectHashMap<>();
                for (byte entry : tmpInterestsIndex.keys()) {
                    interestsIndex.put(entry, new int[tmpInterestsIndex.get(entry)]);
                    tmpInterestsIndex.put(entry, 0);
                }
                fnameIndex = new TIntObjectHashMap<>();
                for (int entry : tmpFnameIndex.keys()) {
                    fnameIndex.put(entry, new int[tmpFnameIndex.get(entry)]);
                    tmpFnameIndex.put(entry, 0);
                }
                emailDomainIndex = new HashMap<>();
                for (Object entry: tmpEmailDomainIndex.keys()) {
                    emailDomainIndex.put((String)entry, new int[tmpEmailDomainIndex.get(entry)]);
                    tmpEmailDomainIndex.put((String) entry, 0);
                }
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                        countryIndex.get(account.country)[tmpCountryIndex.get(account.country)] = account.id;
                        tmpCountryIndex.put(account.country, tmpCountryIndex.get(account.country) + 1);
                        notNullCountry[notNullCountryCounter++] = account.id;
                    } else {
                        nullCountry[nullCountryCounter++] = account.id;
                    }
                    if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        notNullCity[notNullCityCounter++] = account.id;
                    } else {
                        nullCity[nullCityCounter++] = account.id;
                    }
                    statusIndex.get(account.status)[tmpStatusIndex.get(account.status)] = account.id;
                    tmpStatusIndex.put(account.status, tmpStatusIndex.get(account.status) + 1);
                    if (account.interests != null) {
                        for (byte interes : account.interests) {
                            int value = account.id;
                            if (account.sex) {
                                value |= 1 << 31;
                            }
                            interestsIndex.get(interes)[tmpInterestsIndex.get(interes)] = value;
                            tmpInterestsIndex.put(interes, tmpInterestsIndex.get(interes) + 1);
                        }
                    }
                    int at = account.email.lastIndexOf('@');
                    long hash = calculateHash(account.email, at + 1 , account.email.length());
                    String domain = domainsMap.get(hash);
                    if (domain == null) {
                        domain = account.email.substring(at + 1);
                        domainsMap.put(hash, domain);
                    }
                    emailDomainIndex.get(domain)[tmpEmailDomainIndex.get(domain)] = account.id;
                    tmpEmailDomainIndex.put(domain, tmpEmailDomainIndex.get(domain) + 1);
                    if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        notNullFname[notNullFnameCounter++] = account.id;
                        fnameIndex.get(account.fname)[tmpFnameIndex.get(account.fname)] = account.id;
                        tmpFnameIndex.put(account.fname, tmpFnameIndex.get(account.fname) + 1);
                    } else {
                        nullFname[nullFnameCounter++] = account.id;
                    }
                    if (account.premium) {
                        premiumIndex[premiumCounter++] = account.id;
                    }
                }
                System.out.println("Finish init IndexHolder " + new Date());
                return true;
            }
        };
        Callable<Boolean> task2 = new Callable<Boolean>() {
            @Override
            public Boolean call() {
                phoneIndex = null;
                sexIndex = null;
                notNullSname = null;
                nullSname = null;
                notNullPhone = null;
                nullPhone = null;
                notNullPremium = null;
                nullPremium = null;
                cityIndex = null;
                snameIndex = null;
                birthYearIndex = null;
                phoneCodeIndex = null;
                joinedIndex = null;
                TByteIntMap tmpSexIndex = new TByteIntHashMap();
                TObjectIntMap<String> tmpPhoneCodeIndex = new TObjectIntHashMap<>();
                int notNullSnameCounter = 0;
                int nullSnameCounter = 0;
                int notNullPhoneCounter = 0;
                int nullPhoneCounter = 0;
                int notNullPremiumCounter = 0;
                int nullPremiumCounter = 0;
                TIntIntMap tmpBirthYearIndex = new TIntIntHashMap();
                tmpSexIndex.put((byte)1, 0);
                tmpSexIndex.put((byte)0, 0);
                TIntIntMap tmpCityIndex = new TIntIntHashMap();
                TIntIntMap tmpSnameIndex = new TIntIntHashMap();
                TIntIntMap tmpJoinedIndex = new TIntIntHashMap();
                phoneIndex = new TObjectIntHashMap<>();
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        int count = tmpCityIndex.get(account.city);
                        if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                            tmpCityIndex.put(account.city, 1);
                        } else {
                            tmpCityIndex.put(account.city, count + 1);
                        }
                    }
                    tmpSexIndex.put(account.sex ? (byte)1 : 0, tmpSexIndex.get(account.sex ? (byte)1 : 0) + 1);

                    int year = BirthYearPredicate.calculateYear( account.birth);
                    int count = tmpBirthYearIndex.get(year);
                    if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                        tmpBirthYearIndex.put(year, 1);
                    } else {
                        tmpBirthYearIndex.put(year, tmpBirthYearIndex.get(year) + 1);
                    }
                    if (account.joined != Integer.MIN_VALUE) {
                        int jyear = JoinedYearPredicate.calculateYear(account.joined);
                        count = tmpJoinedIndex.get(jyear);
                        if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                            tmpJoinedIndex.put(jyear, 1);
                        } else {
                            tmpJoinedIndex.put(jyear, tmpJoinedIndex.get(jyear) + 1);
                        }
                    }
                    if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        notNullSnameCounter++;
                        count = tmpSnameIndex.get(account.sname);
                        if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                            tmpSnameIndex.put(account.sname, 1);
                        } else {
                            tmpSnameIndex.put(account.sname, tmpSnameIndex.get(account.sname) + 1);
                        }
                    } else {
                        nullSnameCounter++;
                    }
                    if (account.phone != null) {
                        phoneIndex.put(account.phone, account.id);
                        notNullPhoneCounter++;
                        int open = account.phone.indexOf("(");
                        if (open != -1) {
                            int close = account.phone.indexOf(')', open + 1);
                            if (close != -1) {
                                long hash = calculateHash(account.phone, open + 1, close);
                                String code = phoneCodeMap.get(hash);
                                if (code == null) {
                                    code = account.phone.substring(open + 1, close);
                                    phoneCodeMap.put(hash, code);
                                }
                                if (!tmpPhoneCodeIndex.containsKey(code)) {
                                    tmpPhoneCodeIndex.put(code, 1);
                                } else  {
                                    tmpPhoneCodeIndex.put(code, tmpPhoneCodeIndex.get(code) + 1);
                                }
                            }
                        }
                    } else {
                        nullPhoneCounter++;
                    }
                    if (account.premiumStart != 0) {
                        notNullPremiumCounter++;
                    } else {
                        nullPremiumCounter++;
                    }
                }
                sexIndex = new TByteObjectHashMap<>();
                for (byte entry : tmpSexIndex.keys()) {
                    sexIndex.put(entry, new int[tmpSexIndex.get(entry)]);
                    tmpSexIndex.put(entry, 0);
                }
                notNullSname = new int[notNullSnameCounter];
                nullSname = new int[nullSnameCounter];
                notNullPhone = new int[notNullPhoneCounter];
                nullPhone = new int[nullPhoneCounter];
                notNullPremium = new int[notNullPremiumCounter];
                nullPremium = new int[nullPremiumCounter];
                notNullSnameCounter = 0;
                nullSnameCounter = 0;
                notNullPhoneCounter = 0;
                nullPhoneCounter = 0;
                notNullPremiumCounter = 0;
                nullPremiumCounter = 0;
                cityIndex = new TIntObjectHashMap<>();
                for (int entry : tmpCityIndex.keys()) {
                    cityIndex.put(entry, new int[tmpCityIndex.get(entry)]);
                    tmpCityIndex.put(entry, 0);
                }
                snameIndex = new TIntObjectHashMap<>();
                for (int entry : tmpSnameIndex.keys()) {
                    snameIndex.put(entry, new int[tmpSnameIndex.get(entry)]);
                    tmpSnameIndex.put(entry, 0);
                }
                birthYearIndex = new TIntObjectHashMap<>();
                for (int entry : tmpBirthYearIndex.keys()) {
                    birthYearIndex.put(entry, new int[tmpBirthYearIndex.get(entry)]);
                    tmpBirthYearIndex.put(entry, 0);
                }
                phoneCodeIndex = new HashMap<>();
                for (Object entry :  tmpPhoneCodeIndex.keys()) {
                    phoneCodeIndex.put((String) entry, new int[tmpPhoneCodeIndex.get(entry)]);
                    tmpPhoneCodeIndex.put((String)entry, 0);
                }
                joinedIndex = new TIntObjectHashMap<>();
                for (int entry : tmpJoinedIndex.keys()) {
                    joinedIndex.put(entry, new int[tmpJoinedIndex.get(entry)]);
                    tmpJoinedIndex.put(entry, 0);
                }
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        cityIndex.get(account.city)[tmpCityIndex.get(account.city)] = account.id;
                        tmpCityIndex.put(account.city, tmpCityIndex.get(account.city) + 1);
                    }
                    sexIndex.get(account.sex ? (byte)1 : 0)[tmpSexIndex.get(account.sex ? (byte)1 : 0)] = account.id;
                    tmpSexIndex.put(account.sex ? (byte)1 : 0, tmpSexIndex.get(account.sex ? (byte)1 : 0) + 1);
                    if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                        notNullSname[notNullSnameCounter++] = account.id;
                        snameIndex.get(account.sname)[tmpSnameIndex.get(account.sname)] = account.id;
                        tmpSnameIndex.put(account.sname, tmpSnameIndex.get(account.sname) + 1);
                    } else {
                        nullSname[nullSnameCounter++] = account.id;
                    }
                    int year = BirthYearPredicate.calculateYear( account.birth);
                    birthYearIndex.get(year)[tmpBirthYearIndex.get(year)] = account.id;
                    tmpBirthYearIndex.put(year, tmpBirthYearIndex.get(year) + 1);
                    if (account.phone != null) {
                        notNullPhone[notNullPhoneCounter++] = account.id;
                        int open = account.phone.indexOf("(");
                        if (open != -1) {
                            int close = account.phone.indexOf(')', open + 1);
                            if (close != -1) {
                                long hash = calculateHash(account.phone, open + 1, close);
                                String code = phoneCodeMap.get(hash);
                                if (code == null) {
                                    code = account.phone.substring(open + 1, close);
                                    phoneCodeMap.put(hash, code);
                                }
                                phoneCodeIndex.get(code)[tmpPhoneCodeIndex.get(code)] = account.id;
                                tmpPhoneCodeIndex.put(code, tmpPhoneCodeIndex.get(code) + 1);
                            }
                        }
                    } else {
                        nullPhone[nullPhoneCounter++] = account.id;
                    }
                    if (account.premiumStart != 0) {
                        notNullPremium[notNullPremiumCounter++] = account.id;
                    } else {
                        nullPremium[nullPremiumCounter++] = account.id;
                    }
                    if (account.joined != Integer.MIN_VALUE) {
                        int jyear = JoinedYearPredicate.calculateYear(account.joined);
                        joinedIndex.get(jyear)[tmpJoinedIndex.get(jyear)] = account.id;
                        tmpJoinedIndex.put(jyear, tmpJoinedIndex.get(jyear) + 1);
                    }
                }

                System.out.println("Finish init IndexHolder " + new Date());
                return true;
            }
        };
        Callable<Boolean> task3 = new Callable<Boolean>() {
            @Override
            public Boolean call() {
                long t1 = System.currentTimeMillis();
                if (tIntListTIntObjectMap == null) {
                    System.out.println("Phase1");
                    likesIndex = null;
                    for (int i = 0; i < size; i++) {
                        Account account = accountDTOList[i];
                        if (account.likes != null && account.likes.length != 0) {
                            int prev = -1;
                            for (long like : account.likes) {
                                int id = (int) (like >> 32);
                                if (prev == id) {
                                    continue;
                                }
                                LIKE_TMP_ARRAY[id]++;
                                prev = id;
                            }
                        }
                    }
                    System.out.println("Phase2");

                    long t2 = System.currentTimeMillis();

                    likesIndex = new int[AccountService.MAX_ID][];
                    for (int i = 0; i < AccountService.MAX_ID; i++) {
                        int count = LIKE_TMP_ARRAY[i];
                        if (count != 0) {
                            likesIndex[i] = new int[count];
                            LIKE_TMP_ARRAY[i] = 0;
                        }
                    }

                    long t3 = System.currentTimeMillis();
                    System.out.println("Phase3");
                    for (int i = 0; i < size; i++) {
                        Account account = accountDTOList[i];
                        if (account.likes != null && account.likes.length != 0) {
                            int prev = -1;
                            for (long like : account.likes) {
                                int id = (int) (like >> 32);
                                if (prev == id) {
                                    continue;
                                }
                                likesIndex[id][LIKE_TMP_ARRAY[id]] = account.id;
                                LIKE_TMP_ARRAY[id]++;
                                prev = id;
                            }
                        }
                    }
                    long t4 = System.currentTimeMillis();
                    System.out.println(t2 - t1);
                    System.out.println(t3 - t2);
                    System.out.println(t4 - t3);
                } else {
                    System.out.println("Size=" + tIntListTIntObjectMap.size());
                    int cnt = 0;
                    long t11 = System.currentTimeMillis();
                    for (int id:  tIntListTIntObjectMap.keys()) {
                        TIntList tIntList = tIntListTIntObjectMap.get(id);
                        if (likesIndex[id] == null) {
                            int[] array = tIntList.toArray();
                            Arrays.sort(array);
                            reverse(array, 0, array.length);
                            likesIndex[id] = array;
                            cnt+=array.length;
                        } else {
                            int[] oldArray = likesIndex[id];
                            int oldCount = oldArray.length;
                            int newSize = oldCount + tIntList.size();
                            int[] newArray = new int[newSize];
                            System.arraycopy(oldArray, 0, newArray, 0, oldCount);
                            for (int j = oldCount; j < newSize; j++) {
                                newArray[j] = tIntList.get(j - oldCount);
                            }
                            Arrays.sort(newArray);
                            reverse(newArray, 0, newArray.length);
                            likesIndex[id] = newArray;
                            cnt+=tIntList.size();
                        }
                    }
                    long t22 = System.currentTimeMillis();
                    System.out.println("Cnt=" + cnt);
                    System.out.println("Like index update took " + (t22-t11));
                }
                return true;
            }
        };
        System.out.println("Start tasks " + new Date());
        long t1 = System.currentTimeMillis();
        //executorService.submit(task1).get();
        //executorService.submit(task2).get();
        //executorService.submit(task3).get();
        executorService.invokeAll(Arrays.asList(task1, task2, task3));
        long t2 = System.currentTimeMillis();
        System.out.println("Finish tasks " + new Date() + " took " + (t2-t1));
    }


    public void resetTempListArray() {
        for (int i = 0; i < AccountService.MAX_ID; i++) {
            LIKE_TMP_ARRAY[i] = 0;
        }
    }

    private long calculateHash(String values, int from, int to) {
        long hash = 0;
        for (int i = from; i < to; i++) {
            hash = 31* hash + values.charAt(i);
        }
        return hash;
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

}
