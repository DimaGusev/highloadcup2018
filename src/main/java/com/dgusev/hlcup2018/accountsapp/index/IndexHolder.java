package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.model.Group;
import com.dgusev.hlcup2018.accountsapp.predicate.BirthYearPredicate;
import com.dgusev.hlcup2018.accountsapp.predicate.JoinedYearPredicate;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import com.dgusev.hlcup2018.accountsapp.service.ConvertorUtills;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
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

    public int[] nullCountry;
    public int[] nullCity;
    public int[] nullFname;
    public int[] nullSname;
    public int[] notNullPhone;
    public int[] notNullPremium;
    public int[] premiumIndex;
    public int[][] likesIndex;

    public TByteObjectMap<int[]> sexFalsePremiumState0Index;
    public TByteObjectMap<int[]> sexFalsePremiumState1Index;
    public TByteObjectMap<int[]> sexFalsePremiumState2Index;
    public TByteObjectMap<int[]> sexFalseNonPremiumState0Index;
    public TByteObjectMap<int[]> sexFalseNonPremiumState1Index;
    public TByteObjectMap<int[]> sexFalseNonPremiumState2Index;
    public TByteObjectMap<int[]> sexTruePremiumState0Index;
    public TByteObjectMap<int[]> sexTruePremiumState1Index;
    public TByteObjectMap<int[]> sexTruePremiumState2Index;
    public TByteObjectMap<int[]> sexTrueNonPremiumState0Index;
    public TByteObjectMap<int[]> sexTrueNonPremiumState1Index;
    public TByteObjectMap<int[]> sexTrueNonPremiumState2Index;

    public final byte[] birthYear = new byte[AccountService.MAX_ID];
    public final byte[] joinedYear = new byte[AccountService.MAX_ID];

    @Autowired
    private NowProvider nowProvider;

    @Autowired
    private Dictionary dictionary;

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
                nullCountry = null;
                nullCity = null;
                nullFname = null;
                fnameIndex = null;
                premiumIndex = null;
                emailDomainIndex = null;
                sexFalsePremiumState0Index = null;
                sexFalsePremiumState1Index = null;
                sexFalsePremiumState2Index = null;
                sexFalseNonPremiumState0Index = null;
                sexFalseNonPremiumState1Index = null;
                sexFalseNonPremiumState2Index = null;
                sexTruePremiumState0Index = null;
                sexTruePremiumState1Index = null;
                sexTruePremiumState2Index = null;
                sexTrueNonPremiumState0Index = null;
                sexTrueNonPremiumState1Index = null;
                sexTrueNonPremiumState2Index = null;
                TByteIntMap tmpCountryIndex = new TByteIntHashMap();
                TObjectIntMap<String> tmpEmailDomainIndex = new TObjectIntHashMap<>();
                int nullCountryCounter = 0;
                int nullCityCounter = 0;
                int nullFnameCounter = 0;
                int premiumCounter = 0;
                TByteIntMap tmpStatusIndex = new TByteIntHashMap();
                for (int i = 0; i < 3; i++) {
                    tmpStatusIndex.put((byte) i, 0);
                }
                TByteIntMap tmpInterestsIndex = new TByteIntHashMap();
                TIntIntMap tmpFnameIndex = new TIntIntHashMap();
                emailIndex = new TObjectIntHashMap<>();
                TByteIntMap tmpSexFalsePremiumState0Index = new TByteIntHashMap();
                TByteIntMap tmpSexFalsePremiumState1Index = new TByteIntHashMap();
                TByteIntMap tmpSexFalsePremiumState2Index = new TByteIntHashMap();
                TByteIntMap tmpSexFalseNonPremiumState0Index = new TByteIntHashMap();
                TByteIntMap tmpSexFalseNonPremiumState1Index = new TByteIntHashMap();
                TByteIntMap tmpSexFalseNonPremiumState2Index = new TByteIntHashMap();
                TByteIntMap tmpSexTruePremiumState0Index = new TByteIntHashMap();
                TByteIntMap tmpSexTruePremiumState1Index = new TByteIntHashMap();
                TByteIntMap tmpSexTruePremiumState2Index = new TByteIntHashMap();
                TByteIntMap tmpSexTrueNonPremiumState0Index = new TByteIntHashMap();
                TByteIntMap tmpSexTrueNonPremiumState1Index = new TByteIntHashMap();
                TByteIntMap tmpSexTrueNonPremiumState2Index = new TByteIntHashMap();
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                        int count = tmpCountryIndex.get(account.country);
                        if (count == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                            tmpCountryIndex.put(account.country, 1);
                        } else {
                            tmpCountryIndex.put(account.country, count + 1);
                        }
                    } else {
                        nullCountryCounter++;
                    }
                    if (account.city == Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
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
                            if (account.sex) {
                                if (account.premium) {
                                    if (account.status == 0) {
                                        int cnt = tmpSexTruePremiumState0Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexTruePremiumState0Index.put(interes, 1);
                                        } else {
                                            tmpSexTruePremiumState0Index.put(interes, cnt + 1);
                                        }
                                    } else if (account.status == 1) {
                                        int cnt = tmpSexTruePremiumState1Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexTruePremiumState1Index.put(interes, 1);
                                        } else {
                                            tmpSexTruePremiumState1Index.put(interes, cnt + 1);
                                        }
                                    } else {
                                        int cnt = tmpSexTruePremiumState2Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexTruePremiumState2Index.put(interes, 1);
                                        } else {
                                            tmpSexTruePremiumState2Index.put(interes, cnt + 1);
                                        }
                                    }
                                } else  {
                                    if (account.status == 0) {
                                        int cnt = tmpSexTrueNonPremiumState0Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexTrueNonPremiumState0Index.put(interes, 1);
                                        } else {
                                            tmpSexTrueNonPremiumState0Index.put(interes, cnt + 1);
                                        }
                                    } else if (account.status == 1) {
                                        int cnt = tmpSexTrueNonPremiumState1Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexTrueNonPremiumState1Index.put(interes, 1);
                                        } else {
                                            tmpSexTrueNonPremiumState1Index.put(interes, cnt + 1);
                                        }
                                    } else {
                                        int cnt = tmpSexTrueNonPremiumState2Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexTrueNonPremiumState2Index.put(interes, 1);
                                        } else {
                                            tmpSexTrueNonPremiumState2Index.put(interes, cnt + 1);
                                        }
                                    }
                                }
                            } else {
                                if (account.premium) {
                                    if (account.status == 0) {
                                        int cnt = tmpSexFalsePremiumState0Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexFalsePremiumState0Index.put(interes, 1);
                                        } else {
                                            tmpSexFalsePremiumState0Index.put(interes, cnt + 1);
                                        }
                                    } else if (account.status == 1) {
                                        int cnt = tmpSexFalsePremiumState1Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexFalsePremiumState1Index.put(interes, 1);
                                        } else {
                                            tmpSexFalsePremiumState1Index.put(interes, cnt + 1);
                                        }
                                    } else {
                                        int cnt = tmpSexFalsePremiumState2Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexFalsePremiumState2Index.put(interes, 1);
                                        } else {
                                            tmpSexFalsePremiumState2Index.put(interes, cnt + 1);
                                        }
                                    }
                                } else  {
                                    if (account.status == 0) {
                                        int cnt = tmpSexFalseNonPremiumState0Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexFalseNonPremiumState0Index.put(interes, 1);
                                        } else {
                                            tmpSexFalseNonPremiumState0Index.put(interes, cnt + 1);
                                        }
                                    } else if (account.status == 1) {
                                        int cnt = tmpSexFalseNonPremiumState1Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexFalseNonPremiumState1Index.put(interes, 1);
                                        } else {
                                            tmpSexFalseNonPremiumState1Index.put(interes, cnt + 1);
                                        }
                                    } else {
                                        int cnt = tmpSexFalseNonPremiumState2Index.get(interes);
                                        if (cnt == Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                                            tmpSexFalseNonPremiumState2Index.put(interes, 1);
                                        } else {
                                            tmpSexFalseNonPremiumState2Index.put(interes, cnt + 1);
                                        }
                                    }
                                }
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
                    birthYear[account.id] = (byte)(BirthYearPredicate.calculateYear(account.birth) - 1900);
                    joinedYear[account.id] = (byte)(JoinedYearPredicate.calculateYear(account.joined) - 2000);
                }

                nullCountry = new int[nullCountryCounter];
                nullCity = new int[nullCityCounter];
                nullFname = new int[nullFnameCounter];
                premiumIndex = new int[premiumCounter];
                nullCountryCounter = 0;
                nullCityCounter = 0;
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
                sexFalsePremiumState0Index = new TByteObjectHashMap<>();
                sexFalsePremiumState1Index = new TByteObjectHashMap<>();
                sexFalsePremiumState2Index = new TByteObjectHashMap<>();
                sexFalseNonPremiumState0Index = new TByteObjectHashMap<>();
                sexFalseNonPremiumState1Index = new TByteObjectHashMap<>();
                sexFalseNonPremiumState2Index = new TByteObjectHashMap<>();
                sexTruePremiumState0Index = new TByteObjectHashMap<>();
                sexTruePremiumState1Index = new TByteObjectHashMap<>();
                sexTruePremiumState2Index = new TByteObjectHashMap<>();
                sexTrueNonPremiumState0Index = new TByteObjectHashMap<>();
                sexTrueNonPremiumState1Index = new TByteObjectHashMap<>();
                sexTrueNonPremiumState2Index = new TByteObjectHashMap<>();
                for (byte entry : tmpSexFalsePremiumState0Index.keys()) {
                    sexFalsePremiumState0Index.put(entry, new int[tmpSexFalsePremiumState0Index.get(entry)]);
                    tmpSexFalsePremiumState0Index.put(entry, 0);
                }
                for (byte entry : tmpSexFalsePremiumState1Index.keys()) {
                    sexFalsePremiumState1Index.put(entry, new int[tmpSexFalsePremiumState1Index.get(entry)]);
                    tmpSexFalsePremiumState1Index.put(entry, 0);
                }
                for (byte entry : tmpSexFalsePremiumState2Index.keys()) {
                    sexFalsePremiumState2Index.put(entry, new int[tmpSexFalsePremiumState2Index.get(entry)]);
                    tmpSexFalsePremiumState2Index.put(entry, 0);
                }
                for (byte entry : tmpSexFalseNonPremiumState0Index.keys()) {
                    sexFalseNonPremiumState0Index.put(entry, new int[tmpSexFalseNonPremiumState0Index.get(entry)]);
                    tmpSexFalseNonPremiumState0Index.put(entry, 0);
                }
                for (byte entry : tmpSexFalseNonPremiumState1Index.keys()) {
                    sexFalseNonPremiumState1Index.put(entry, new int[tmpSexFalseNonPremiumState1Index.get(entry)]);
                    tmpSexFalseNonPremiumState1Index.put(entry, 0);
                }
                for (byte entry : tmpSexFalseNonPremiumState2Index.keys()) {
                    sexFalseNonPremiumState2Index.put(entry, new int[tmpSexFalseNonPremiumState2Index.get(entry)]);
                    tmpSexFalseNonPremiumState2Index.put(entry, 0);
                }
                for (byte entry : tmpSexTruePremiumState0Index.keys()) {
                    sexTruePremiumState0Index.put(entry, new int[tmpSexTruePremiumState0Index.get(entry)]);
                    tmpSexTruePremiumState0Index.put(entry, 0);
                }
                for (byte entry : tmpSexTruePremiumState1Index.keys()) {
                    sexTruePremiumState1Index.put(entry, new int[tmpSexTruePremiumState1Index.get(entry)]);
                    tmpSexTruePremiumState1Index.put(entry, 0);
                }
                for (byte entry : tmpSexTruePremiumState2Index.keys()) {
                    sexTruePremiumState2Index.put(entry, new int[tmpSexTruePremiumState2Index.get(entry)]);
                    tmpSexTruePremiumState2Index.put(entry, 0);
                }
                for (byte entry : tmpSexTrueNonPremiumState0Index.keys()) {
                    sexTrueNonPremiumState0Index.put(entry, new int[tmpSexTrueNonPremiumState0Index.get(entry)]);
                    tmpSexTrueNonPremiumState0Index.put(entry, 0);
                }
                for (byte entry : tmpSexTrueNonPremiumState1Index.keys()) {
                    sexTrueNonPremiumState1Index.put(entry, new int[tmpSexTrueNonPremiumState1Index.get(entry)]);
                    tmpSexTrueNonPremiumState1Index.put(entry, 0);
                }
                for (byte entry : tmpSexTrueNonPremiumState2Index.keys()) {
                    sexTrueNonPremiumState2Index.put(entry, new int[tmpSexTrueNonPremiumState2Index.get(entry)]);
                    tmpSexTrueNonPremiumState2Index.put(entry, 0);
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
                    } else {
                        nullCountry[nullCountryCounter++] = account.id;
                    }
                    if (account.city == Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
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
                            if (account.sex) {
                                if (account.premium) {
                                    if (account.status == 0) {
                                        sexTruePremiumState0Index.get(interes)[tmpSexTruePremiumState0Index.get(interes)] = account.id;
                                        tmpSexTruePremiumState0Index.put(interes, tmpSexTruePremiumState0Index.get(interes) + 1);
                                    } else if (account.status == 1) {
                                        sexTruePremiumState1Index.get(interes)[tmpSexTruePremiumState1Index.get(interes)] = account.id;
                                        tmpSexTruePremiumState1Index.put(interes, tmpSexTruePremiumState1Index.get(interes) + 1);
                                    } else {
                                        sexTruePremiumState2Index.get(interes)[tmpSexTruePremiumState2Index.get(interes)] = account.id;
                                        tmpSexTruePremiumState2Index.put(interes, tmpSexTruePremiumState2Index.get(interes) + 1);
                                    }
                                } else  {
                                    if (account.status == 0) {
                                        sexTrueNonPremiumState0Index.get(interes)[tmpSexTrueNonPremiumState0Index.get(interes)] = account.id;
                                        tmpSexTrueNonPremiumState0Index.put(interes, tmpSexTrueNonPremiumState0Index.get(interes) + 1);
                                    } else if (account.status == 1) {
                                        sexTrueNonPremiumState1Index.get(interes)[tmpSexTrueNonPremiumState1Index.get(interes)] = account.id;
                                        tmpSexTrueNonPremiumState1Index.put(interes, tmpSexTrueNonPremiumState1Index.get(interes) + 1);
                                    } else {
                                        sexTrueNonPremiumState2Index.get(interes)[tmpSexTrueNonPremiumState2Index.get(interes)] = account.id;
                                        tmpSexTrueNonPremiumState2Index.put(interes, tmpSexTrueNonPremiumState2Index.get(interes) + 1);
                                    }
                                }
                            } else {
                                if (account.premium) {
                                    if (account.status == 0) {
                                        sexFalsePremiumState0Index.get(interes)[tmpSexFalsePremiumState0Index.get(interes)] = account.id;
                                        tmpSexFalsePremiumState0Index.put(interes, tmpSexFalsePremiumState0Index.get(interes) + 1);
                                    } else if (account.status == 1) {
                                        sexFalsePremiumState1Index.get(interes)[tmpSexFalsePremiumState1Index.get(interes)] = account.id;
                                        tmpSexFalsePremiumState1Index.put(interes, tmpSexFalsePremiumState1Index.get(interes) + 1);
                                    } else {
                                        sexFalsePremiumState2Index.get(interes)[tmpSexFalsePremiumState2Index.get(interes)] = account.id;
                                        tmpSexFalsePremiumState2Index.put(interes, tmpSexFalsePremiumState2Index.get(interes) + 1);
                                    }
                                } else  {
                                    if (account.status == 0) {
                                        sexFalseNonPremiumState0Index.get(interes)[tmpSexFalseNonPremiumState0Index.get(interes)] = account.id;
                                        tmpSexFalseNonPremiumState0Index.put(interes, tmpSexFalseNonPremiumState0Index.get(interes) + 1);
                                    } else if (account.status == 1) {
                                        sexFalseNonPremiumState1Index.get(interes)[tmpSexFalseNonPremiumState1Index.get(interes)] = account.id;
                                        tmpSexFalseNonPremiumState1Index.put(interes, tmpSexFalseNonPremiumState1Index.get(interes) + 1);
                                    } else {
                                        sexFalseNonPremiumState2Index.get(interes)[tmpSexFalseNonPremiumState2Index.get(interes)] = account.id;
                                        tmpSexFalseNonPremiumState2Index.put(interes, tmpSexFalseNonPremiumState2Index.get(interes) + 1);
                                    }
                                }
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
                    emailDomainIndex.get(domain)[tmpEmailDomainIndex.get(domain)] = account.id;
                    tmpEmailDomainIndex.put(domain, tmpEmailDomainIndex.get(domain) + 1);
                    if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
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
                nullSname = null;
                notNullPhone = null;
                notNullPremium = null;
                cityIndex = null;
                snameIndex = null;
                birthYearIndex = null;
                phoneCodeIndex = null;
                joinedIndex = null;
                TByteIntMap tmpSexIndex = new TByteIntHashMap();
                TObjectIntMap<String> tmpPhoneCodeIndex = new TObjectIntHashMap<>();
                int nullSnameCounter = 0;
                int notNullPhoneCounter = 0;
                int notNullPremiumCounter = 0;
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
                    }
                    if (account.premiumStart != 0) {
                        notNullPremiumCounter++;
                    }
                }
                sexIndex = new TByteObjectHashMap<>();
                for (byte entry : tmpSexIndex.keys()) {
                    sexIndex.put(entry, new int[tmpSexIndex.get(entry)]);
                    tmpSexIndex.put(entry, 0);
                }
                nullSname = new int[nullSnameCounter];
                notNullPhone = new int[notNullPhoneCounter];
                notNullPremium = new int[notNullPremiumCounter];
                nullSnameCounter = 0;
                notNullPhoneCounter = 0;
                notNullPremiumCounter = 0;
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
                    }
                    if (account.premiumStart != 0) {
                        notNullPremium[notNullPremiumCounter++] = account.id;
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
        Callable<Boolean> task4 = new GroupsUpdater(accountDTOList, size);
        System.out.println("Start tasks " + new Date());
        long t1 = System.currentTimeMillis();
        //executorService.submit(task1).get();
        //executorService.submit(task2).get();
        //executorService.submit(task3).get();
        executorService.invokeAll(Arrays.asList(task1, task2, task3, task4));
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


    public long[] group1;
    public long[] group2;
    public long[] group3;
    public long[] group4;
    public long[] group5;
    public long[] group6;
    public long[] group7;
    public long[] group8;
    public long[] group9;

    public class GroupsUpdater implements Callable<Boolean> {

        private Account[] accounts;
        private int size;

        public GroupsUpdater(Account[] accountDTOList, int size) {
            this.accounts = accountDTOList;
            this.size = size;
        }

        @Override
        public Boolean call() throws Exception {

            System.out.println("Start groups update" + new Date());
            group1 = null;
            group2 = null;
            group3 = null;
            group4 = null;
            group5 = null;
            group6 = null;
            group7 = null;
            group8 = null;
            group9 = null;
            long t1 = System.currentTimeMillis();
            byte mask1 = 0b00000100;
            byte mask2 = 0b00001001;
            byte mask3 = 0b00001000;
            byte mask4 = 0b00000010;
            byte mask5 = 0b00010001;
            byte mask6 = 0b00000001;
            byte mask7 = 0b00010000;
            byte mask8 = 0b00001010;
            byte mask9 = 0b00010010;
            List<String> keys1 = Arrays.asList("interests");
            List<String> keys2 = Arrays.asList("country", "sex");
            List<String> keys3 = Arrays.asList("country");
            List<String> keys4 = Arrays.asList("status");
            List<String> keys5 = Arrays.asList("city", "sex");
            List<String> keys6 = Arrays.asList("sex");
            List<String> keys7 = Arrays.asList("city");
            List<String> keys8 = Arrays.asList("country", "status");
            List<String> keys9 = Arrays.asList("city", "status");

            TIntIntHashMap groupsCount1 = new TIntIntHashMap();
            TIntIntHashMap groupsCount2 = new TIntIntHashMap();
            TIntIntHashMap groupsCount3 = new TIntIntHashMap();
            TIntIntHashMap groupsCount4 = new TIntIntHashMap();
            TIntIntHashMap groupsCount5 = new TIntIntHashMap();
            TIntIntHashMap groupsCount6 = new TIntIntHashMap();
            TIntIntHashMap groupsCount7 = new TIntIntHashMap();
            TIntIntHashMap groupsCount8 = new TIntIntHashMap();
            TIntIntHashMap groupsCount9 = new TIntIntHashMap();

            for (int i = 0; i < size; i++) {
                Account account = accounts[i];
                processRecord3(account, groupsCount1, mask1);
                processRecord3(account, groupsCount2, mask2);
                processRecord3(account, groupsCount3, mask3);
                processRecord3(account, groupsCount4, mask4);
                processRecord3(account, groupsCount5, mask5);
                processRecord3(account, groupsCount6, mask6);
                processRecord3(account, groupsCount7, mask7);
                processRecord3(account, groupsCount8, mask8);
                processRecord3(account, groupsCount9, mask9);
            }
            Group[] group1Array = new Group[groupsCount1.size()];
            Group[] group2Array = new Group[groupsCount2.size()];
            Group[] group3Array = new Group[groupsCount3.size()];
            Group[] group4Array = new Group[groupsCount4.size()];
            Group[] group5Array = new Group[groupsCount5.size()];
            Group[] group6Array = new Group[groupsCount6.size()];
            Group[] group7Array = new Group[groupsCount7.size()];
            Group[] group8Array = new Group[groupsCount8.size()];
            Group[] group9Array = new Group[groupsCount9.size()];
            Comparator<Group> comparator1 = new GroupsComparator(keys1);
            Comparator<Group> comparator2 = new GroupsComparator(keys2);
            Comparator<Group> comparator3 = new GroupsComparator(keys3);
            Comparator<Group> comparator4 = new GroupsComparator(keys4);
            Comparator<Group> comparator5 = new GroupsComparator(keys5);
            Comparator<Group> comparator6 = new GroupsComparator(keys6);
            Comparator<Group> comparator7 = new GroupsComparator(keys7);
            Comparator<Group> comparator8 = new GroupsComparator(keys8);
            Comparator<Group> comparator9 = new GroupsComparator(keys9);
            int counter = 0;
            for (int group: groupsCount1.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount1.get(group);
                tmp.values = group;
                group1Array[counter++] = tmp;
            }
            Arrays.sort(group1Array, comparator1);
            group1 = new long[group1Array.length];
            for (int i = 0; i < group1Array.length; i++) {
                Group grp = group1Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group1[i] = value;
            }
            counter = 0;
            for (int group: groupsCount2.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount2.get(group);
                tmp.values = group;
                group2Array[counter++] = tmp;
            }
            Arrays.sort(group2Array, comparator2);
            group2 = new long[group2Array.length];
            for (int i = 0; i < group2Array.length; i++) {
                Group grp = group2Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group2[i] = value;
            }
            counter = 0;
            for (int group: groupsCount3.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount3.get(group);
                tmp.values = group;
                group3Array[counter++] = tmp;
            }
            Arrays.sort(group3Array, comparator3);
            group3 = new long[group3Array.length];
            for (int i = 0; i < group3Array.length; i++) {
                Group grp = group3Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group3[i] = value;
            }
            counter = 0;
            for (int group: groupsCount4.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount4.get(group);
                tmp.values = group;
                group4Array[counter++] = tmp;
            }
            Arrays.sort(group4Array, comparator4);
            group4 = new long[group4Array.length];
            for (int i = 0; i < group4Array.length; i++) {
                Group grp = group4Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group4[i] = value;
            }
            counter = 0;
            for (int group: groupsCount5.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount5.get(group);
                tmp.values = group;
                group5Array[counter++] = tmp;
            }
            Arrays.sort(group5Array, comparator5);
            group5 = new long[group5Array.length];
            for (int i = 0; i < group5Array.length; i++) {
                Group grp = group5Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group5[i] = value;
            }
            counter = 0;
            for (int group: groupsCount6.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount6.get(group);
                tmp.values = group;
                group6Array[counter++] = tmp;
            }
            Arrays.sort(group6Array, comparator6);
            group6 = new long[group6Array.length];
            for (int i = 0; i < group6Array.length; i++) {
                Group grp = group6Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group6[i] = value;
            }
            counter = 0;
            for (int group: groupsCount7.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount7.get(group);
                tmp.values = group;
                group7Array[counter++] = tmp;
            }
            Arrays.sort(group7Array, comparator7);
            group7 = new long[group7Array.length];
            for (int i = 0; i < group7Array.length; i++) {
                Group grp = group7Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group7[i] = value;
            }
            counter = 0;
            for (int group: groupsCount8.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount8.get(group);
                tmp.values = group;
                group8Array[counter++] = tmp;
            }
            Arrays.sort(group8Array, comparator8);
            group8 = new long[group8Array.length];
            for (int i = 0; i < group8Array.length; i++) {
                Group grp = group8Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group8[i] = value;
            }
            counter = 0;
            for (int group: groupsCount9.keys()) {
                Group tmp = new Group();
                tmp.count = groupsCount9.get(group);
                tmp.values = group;
                group9Array[counter++] = tmp;
            }
            Arrays.sort(group9Array, comparator9);
            group9 = new long[group9Array.length];
            for (int i = 0; i < group9Array.length; i++) {
                Group grp = group9Array[i];
                long value = grp.count;
                value|= ((long)grp.values << 32);
                group9[i] = value;
            }

            long t2 = System.currentTimeMillis();
            System.out.println("Finish groups update" + new Date() + " took=" + (t2-t1));
            return true;
        }

        private class GroupsComparator implements Comparator<Group>{

            List<String> keys;

            GroupsComparator(List<String> keys) {
                this.keys = keys;
            }

            @Override
            public int compare(Group o1, Group o2) {
                return GroupsUpdater.this.compare(o1.count, o1.values, o2.count, o2.values, keys,1);
            }
        }

        private void processRecord3(Account account, TIntIntMap groupsCountMap, byte keysMask) {
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
    }



}
