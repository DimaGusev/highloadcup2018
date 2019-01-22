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
import sun.misc.Unsafe;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class IndexHolder {

    private static final Unsafe UNSAFE = com.dgusev.hlcup2018.accountsapp.service.Unsafe.UNSAFE;

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
    public TObjectIntMap<byte[]> emailIndex;
    public TObjectIntMap<byte[]> phoneIndex;
    public Map<String, int[]> phoneCodeIndex;

    public int[] nullCountry;
    public int[] nullCity;
    public int[] nullFname;
    public int[] nullSname;
    public int[] notNullPhone;
    public int[] notNullPremium;
    public int[] premiumIndex;

    public byte[] minEmail = "zzzzzzz".getBytes();


    public long[] likesIndex;

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

    public static final byte[] birthYear = new byte[AccountService.MAX_ID];
    public static final byte[] joinedYear = new byte[AccountService.MAX_ID];

    public static final byte[] masks = new byte[9];
    public static final List[] keys = new List[9];

    static {
        masks[0] = 0b00000100;
        masks[1] = 0b00001001;
        masks[2] = 0b00001000;
        masks[3] = 0b00000010;
        masks[4] = 0b00010001;
        masks[5] = 0b00000001;
        masks[6] = 0b00010000;
        masks[7] = 0b00001010;
        masks[8] = 0b00010010;
        keys[0] = Arrays.asList("interests");
        keys[1] = Arrays.asList("country", "sex");
        keys[2] = Arrays.asList("country");
        keys[3] = Arrays.asList("status");
        keys[4] = Arrays.asList("city", "sex");
        keys[5] = Arrays.asList("sex");
        keys[6] = Arrays.asList("city");
        keys[7] = Arrays.asList("country", "status");
        keys[8] = Arrays.asList("city", "status");
    }


    @Autowired
    private NowProvider nowProvider;

    @Autowired
    private Dictionary dictionary;

    public synchronized void init(Account[] accountDTOList, int size, boolean initLikes) throws ExecutionException, InterruptedException {
        System.out.println("Start init IndexHolder " + new Date());
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        int now = nowProvider.getNow();
        Callable<Boolean> task1 = new Callable<Boolean>() {

            private long calculateHash(byte[] values, int from, int to) {
                long hash = 0;
                for (int i = from; i < to; i++) {
                    hash = 31 * hash + values[i];
                }
                return hash;
            }

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
                    int at = lastIndexOf(account.email, (byte) '@');
                    long hash = calculateHash(account.email, at + 1 , account.email.length);
                    String domain = domainsMap.get(hash);
                    if (domain == null) {
                        domain = substring(account.email, at + 1);
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
                    int at = lastIndexOf(account.email, (byte) '@');
                    long hash = calculateHash(account.email, at + 1 , account.email.length);
                    String domain = domainsMap.get(hash);
                    if (domain == null) {
                        domain = substring(account.email, at + 1);
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

            private int lastIndexOf(byte[] values, byte ch) {

                for (int i = values.length - 1; i>=0; i--) {
                    if (values[i] == ch) {
                        return i;
                    }
                }
                return -1;
            }

            private int indexOf(byte[] values, byte ch) {

                for (int i = 0; i < values.length; i++) {
                    if (values[i] == ch) {
                        return i;
                    }
                }
                return -1;
            }

            private String substring(byte[] values, int from) {
                return substring(values, from, values.length);
            }

            private String substring(byte[] values, int from, int to) {
                byte[] result = new byte[to - from];
                for (int i = from; i < to; i++) {
                    result[i - from] = values[i];
                }
                return new String(result);
            }
        };
        Callable<Boolean> task2 = new Callable<Boolean>() {

            private long calculateHash(byte[] values, int from, int to) {
                long hash = 0;
                for (int i = from; i < to; i++) {
                    hash = 31 * hash + values[i];
                }
                return hash;
            }

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
                        int open = indexOf(account.phone, (byte) '(');
                        if (open != -1) {
                            int close = indexOf(account.phone, (byte) ')', open + 1);
                            if (close != -1) {
                                long hash = calculateHash(account.phone, open + 1, close);
                                String code = phoneCodeMap.get(hash);
                                if (code == null) {
                                    code = substring(account.phone, open + 1, close);
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
                    if (compareTo(account.email, minEmail) < 0) {
                        minEmail = account.email;
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
                        int open = indexOf(account.phone, (byte) '(');
                        if (open != -1) {
                            int close = indexOf(account.phone, (byte) ')', open + 1);
                            if (close != -1) {
                                long hash = calculateHash(account.phone, open + 1, close);
                                String code = phoneCodeMap.get(hash);
                                if (code == null) {
                                    code = substring(account.phone, open + 1, close);
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

            private int indexOf(byte[] values, byte ch) {

                for (int i = 0; i < values.length; i++) {
                    if (values[i] == ch) {
                        return i;
                    }
                }
                return -1;
            }

            private int compareTo(byte[] values1, byte[] values2) {
                int len1 = values1.length;
                int len2 = values2.length;
                int lim = 0;
                if (len1 < len2) {
                    lim = len1;
                } else {
                    lim = len2;
                }
                int k = 0;
                while (k < lim) {
                    byte c1 = values1[k];
                    byte c2 = values2[k];
                    if (c1 != c2) {
                        return c1 - c2;
                    }
                    k++;
                }
                return len1 - len2;
            }

            private int indexOf(byte[] values, byte ch, int from) {
                for (int i = from; i < values.length; i++) {
                    if (values[i] == ch) {
                        return i;
                    }
                }
                return -1;
            }

            private String substring(byte[] values, int from) {
                return substring(values, from, values.length);
            }

            private String substring(byte[] values, int from, int to) {
                byte[] result = new byte[to - from];
                for (int i = from; i < to; i++) {
                    result[i - from] = values[i];
                }
                return new String(result);
            }
        };
        Callable<Boolean> task3 = new Callable<Boolean>() {
            @Override
            public Boolean call() {
                long t1 = System.currentTimeMillis();
                System.out.println("Phase1");
                long p1start = System.nanoTime();
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
                long p1finish = System.nanoTime();
                System.out.println("Phase1 took=" + (p1finish - p1start));
                System.out.println("Phase2");
                long p2start = System.nanoTime();
                likesIndex = new long[AccountService.MAX_ID];
                for (int i = 0; i < AccountService.MAX_ID; i++) {
                    int count = LIKE_TMP_ARRAY[i];
                    if (count != 0) {
                        likesIndex[i] = UNSAFE.allocateMemory(1 + 4 * count);
                        UNSAFE.putByte(likesIndex[i], (byte) count);
                        LIKE_TMP_ARRAY[i] = 0;
                    }
                }
                long p2finish = System.nanoTime();
                System.out.println("Phase2 took=" + (p2finish - p2start));
                System.out.println("Phase3");
                long p3start = System.nanoTime();
                for (int i = 0; i < size; i++) {
                    Account account = accountDTOList[i];
                    if (account.likes != null && account.likes.length != 0) {
                        int prev = -1;
                        for (long like : account.likes) {
                            int id = (int) (like >> 32);
                            if (prev == id) {
                                continue;
                            }
                            long address = likesIndex[id] + 1 + LIKE_TMP_ARRAY[id] * 4;
                            UNSAFE.putInt(address, account.id);
                            LIKE_TMP_ARRAY[id]++;
                            prev = id;
                        }
                    }
                }
                long p3finish = System.nanoTime();
                System.out.println("Phase3 took=" + (p3finish - p3start));
                long t2 = System.currentTimeMillis();
                System.out.println("Like index update took " + (t2 - t1));
                return true;
            }
        };
        Callable<Boolean> task4 = new GroupsUpdater(accountDTOList, size);
        Callable<Boolean> task5 = new AuxiliaryGroupsCalculatorTask(accountDTOList, size);
        System.out.println("Start tasks " + new Date());
        long t1 = System.currentTimeMillis();
        if (initLikes) {
            executorService.invokeAll(Arrays.asList(task1, task2, task3, task4));
            executorService.submit(task5).get();
        } else {
            executorService.invokeAll(Arrays.asList(task1, task2, task4));
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Finish tasks " + new Date() + " took " + (t2 - t1));
    }


    public void resetTempListArray() {
        for (int i = 0; i < AccountService.MAX_ID; i++) {
            LIKE_TMP_ARRAY[i] = 0;
        }
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


    public long[][] groups;
    public long[][][] sexGroups;
    public long[][][] statusGroups;
    public long[][][] joinedGroups;

    public class GroupsUpdater implements Callable<Boolean> {

        private Account[] accounts;
        private int size;

        public GroupsUpdater(Account[] accountDTOList, int size) {
            this.accounts = accountDTOList;
            this.size = size;
        }

        @Override
        public Boolean call() throws Exception {

            try {
                long t1 = System.currentTimeMillis();
                System.out.println("Start groups update" + new Date());
                groups = new long[9][];
                GroupCalculator groupTotalCalculator = new GroupCalculator(groups);
                sexGroups = new long[2][][];
                sexGroups[0] = new long[9][];
                sexGroups[1] = new long[9][];
                GroupCalculator sexFalseGroupCalculator = new GroupCalculator(sexGroups[0]);
                GroupCalculator sexTrueGroupCalculator = new GroupCalculator(sexGroups[1]);
                statusGroups = new long[3][][];
                statusGroups[0] = new long[9][];
                statusGroups[1] = new long[9][];
                statusGroups[2] = new long[9][];
                GroupCalculator status0GroupCalculator = new GroupCalculator(statusGroups[0]);
                GroupCalculator status1GroupCalculator = new GroupCalculator(statusGroups[1]);
                GroupCalculator status2GroupCalculator = new GroupCalculator(statusGroups[2]);
                joinedGroups = new long[7][][];
                GroupCalculator[] joinedCalculators = new GroupCalculator[7];
                for (int i = 0; i< 7; i++) {
                    joinedGroups[i] = new long[9][];
                    joinedCalculators[i] = new GroupCalculator(joinedGroups[i]);
                }
                for (int i = 0; i < size; i++) {
                    Account account = accounts[i];
                    groupTotalCalculator.apply(account);
                    if (account.sex) {
                        sexTrueGroupCalculator.apply(account);
                    } else {
                        sexFalseGroupCalculator.apply(account);
                    }
                    if (account.status == 0) {
                        status0GroupCalculator.apply(account);
                    } else if (account.status == 1) {
                        status1GroupCalculator.apply(account);
                    } else {
                        status2GroupCalculator.apply(account);
                    }
                    int joinedIndex = joinedYear[account.id] - 11;
                    joinedCalculators[joinedIndex].apply(account);
                }
                groupTotalCalculator.complete();
                sexTrueGroupCalculator.complete();
                sexFalseGroupCalculator.complete();
                status0GroupCalculator.complete();
                status1GroupCalculator.complete();
                status2GroupCalculator.complete();
                for (int i = 0; i< 7; i++) {
                    joinedCalculators[i].complete();
                }
                long t2 = System.currentTimeMillis();
                System.out.println("Finish groups update" + new Date() + " took=" + (t2 - t1));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return true;

        }
    }

    public long[][] joinedSexGroupsIndex = new long[14][];
    public long[][] joinedStatusGroupsIndex = new long[21][];
    public long[][] birthGroupsIndex = new long[55][];
    public long[][] birthSexGroupsIndex = new long[110][];
    public long[][] birthStatusGroupsIndex = new long[165][];
    public long[][] interesGroupsIndex = new long[90][];
    public long[][] countryGroupsIndex = new long[70][];

    public class AuxiliaryGroupsCalculatorTask implements Callable<Boolean> {

        private Account[] accounts;
        private int size;

        public AuxiliaryGroupsCalculatorTask(Account[] accountDTOList, int size) {
            this.accounts = accountDTOList;
            this.size = size;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                System.out.println("Start AuxiliaryGroupsCalculatorTask");
                long[][][] joinedSexGroups = new long[14][][];
                GroupCalculator[] joinedSexCalculators = new GroupCalculator[14];
                for (int i = 0; i< 14; i++) {
                    joinedSexGroups[i] = new long[9][];
                    joinedSexCalculators[i] = new GroupCalculator(joinedSexGroups[i]);
                    joinedSexGroupsIndex[i] = new long[9];
                }
                long[][][] joinedStatusGroups = new long[21][][];
                GroupCalculator[] joinedStatusCalculators = new GroupCalculator[21];
                for (int i = 0; i< 21; i++) {
                    joinedStatusGroups[i] = new long[9][];
                    joinedStatusCalculators[i] = new GroupCalculator(joinedStatusGroups[i]);
                    joinedStatusGroupsIndex[i] = new long[9];
                }
                long[][][] birthGroups = new long[55][][];
                GroupCalculator[] birthCalculators = new GroupCalculator[55];
                for (int i = 0; i< 55; i++) {
                    birthGroups[i] = new long[9][];
                    birthCalculators[i] = new GroupCalculator(birthGroups[i]);
                    birthGroupsIndex[i] = new long[9];
                }
                long[][][] birthSexGroups = new long[110][][];
                GroupCalculator[] birthSexCalculators = new GroupCalculator[110];
                for (int i = 0; i< 110; i++) {
                    birthSexGroups[i] = new long[9][];
                    birthSexCalculators[i] = new GroupCalculator(birthSexGroups[i]);
                    birthSexGroupsIndex[i] = new long[9];
                }
                long[][][] birthStatusGroups = new long[165][][];
                GroupCalculator[] birthStatusCalculators = new GroupCalculator[165];
                for (int i = 0; i< 165; i++) {
                    birthStatusGroups[i] = new long[9][];
                    birthStatusCalculators[i] = new GroupCalculator(birthStatusGroups[i]);
                    birthStatusGroupsIndex[i] = new long[9];
                }
                long[][][] interesGroups = new long[90][][];
                GroupCalculator[] interesCalculators = new GroupCalculator[90];
                for (int i = 0; i< 90; i++) {
                    interesGroups[i] = new long[9][];
                    interesCalculators[i] = new GroupCalculator(interesGroups[i]);
                    interesGroupsIndex[i] = new long[9];
                }
                long[][][] countryGroups = new long[70][][];
                GroupCalculator[] countryCalculators = new GroupCalculator[70];
                for (int i = 0; i< 70; i++) {
                    countryGroups[i] = new long[9][];
                    countryCalculators[i] = new GroupCalculator(countryGroups[i]);
                    countryGroupsIndex[i] = new long[9];
                }
                for (int i = 0; i < size; i++) {
                    Account account = accounts[i];
                    int joinedIndex = joinedYear[account.id] - 11;
                    int joinedStatusIndex = joinedIndex;
                    if (account.sex) {
                        joinedIndex+=7;
                    }
                    joinedStatusIndex+=7*account.status;
                    int birthIndex = birthYear[account.id] - 50;
                    int birthSexIndex = birthIndex;
                    int birthStatusIndex = birthIndex;
                    if (account.sex) {
                        birthSexIndex+=55;
                    }
                    birthStatusIndex+=55*account.status;
                    birthCalculators[birthIndex].apply(account);
                    birthSexCalculators[birthSexIndex].apply(account);
                    birthStatusCalculators[birthStatusIndex].apply(account);
                    joinedSexCalculators[joinedIndex].apply(account);
                    joinedStatusCalculators[joinedStatusIndex].apply(account);
                    if (account.interests != null && account.interests.length != 0) {
                        for (byte interes: account.interests) {
                            interesCalculators[interes - 1].apply(account);
                        }
                    }
                    if (account.country != 0) {
                        countryCalculators[account.country - 1].apply(account);
                    }
                }
                for (int i = 0; i< 14; i++) {
                    joinedSexCalculators[i].complete();
                }
                for (int i = 0; i< 21; i++) {
                    joinedStatusCalculators[i].complete();
                }
                for (int i = 0; i< 55; i++) {
                    birthCalculators[i].complete();
                }
                for (int i = 0; i< 110; i++) {
                    birthSexCalculators[i].complete();
                }
                for (int i = 0; i< 165; i++) {
                    birthStatusCalculators[i].complete();
                }
                for (int i = 0; i< 90; i++) {
                    interesCalculators[i].complete();
                }
                for (int i = 0; i< 70; i++) {
                    countryCalculators[i].complete();
                }

                long size = 0;
                for (int i = 0; i < 14; i++) {
                   for (int j = 0; j < 9; j++) {
                       if (joinedSexGroups[i][j].length != 0) {
                           long address = UNSAFE.allocateMemory(2 + 8*joinedSexGroups[i][j].length);
                           size+=2 + 8*joinedSexGroups[i][j].length;
                           joinedSexGroupsIndex[i][j] = address;
                           UNSAFE.putShort(address, (short) joinedSexGroups[i][j].length);
                           address+=2;
                           for (int k = 0; k < joinedSexGroups[i][j].length; k++) {
                               UNSAFE.putLong(address, joinedSexGroups[i][j][k]);
                               address+=8;
                           }
                       }
                   }
                }
                for (int i = 0; i < 21; i++) {
                    for (int j = 0; j < 9; j++) {
                        if (joinedStatusGroups[i][j].length != 0) {
                            long address = UNSAFE.allocateMemory(2 + 8*joinedStatusGroups[i][j].length);
                            size+=2 + 8*joinedStatusGroups[i][j].length;
                            joinedStatusGroupsIndex[i][j] = address;
                            UNSAFE.putShort(address, (short) joinedStatusGroups[i][j].length);
                            address+=2;
                            for (int k = 0; k < joinedStatusGroups[i][j].length; k++) {
                                UNSAFE.putLong(address, joinedStatusGroups[i][j][k]);
                                address+=8;
                            }
                        }
                    }
                }
                for (int i = 0; i < 55; i++) {
                    for (int j = 0; j < 9; j++) {
                        if (birthGroups[i][j].length != 0) {
                            long address = UNSAFE.allocateMemory(2 + 8*birthGroups[i][j].length);
                            size+=2 + 8*birthGroups[i][j].length;
                            birthGroupsIndex[i][j] = address;
                            UNSAFE.putShort(address, (short) birthGroups[i][j].length);
                            address+=2;
                            for (int k = 0; k < birthGroups[i][j].length; k++) {
                                UNSAFE.putLong(address, birthGroups[i][j][k]);
                                address+=8;
                            }
                        }
                    }
                }

                for (int i = 0; i < 110; i++) {
                    for (int j = 0; j < 9; j++) {
                        if (birthSexGroups[i][j].length != 0) {
                            long address = UNSAFE.allocateMemory(2 + 8*birthSexGroups[i][j].length);
                            size+=2 + 8*birthSexGroups[i][j].length;
                            birthSexGroupsIndex[i][j] = address;
                            UNSAFE.putShort(address, (short) birthSexGroups[i][j].length);
                            address+=2;
                            for (int k = 0; k < birthSexGroups[i][j].length; k++) {
                                UNSAFE.putLong(address, birthSexGroups[i][j][k]);
                                address+=8;
                            }
                        }
                    }
                }

                for (int i = 0; i < 165; i++) {
                    for (int j = 0; j < 9; j++) {
                        if (birthStatusGroups[i][j].length != 0) {
                            long address = UNSAFE.allocateMemory(2 + 8*birthStatusGroups[i][j].length);
                            size+=2 + 8*birthStatusGroups[i][j].length;
                            birthStatusGroupsIndex[i][j] = address;
                            UNSAFE.putShort(address, (short) birthStatusGroups[i][j].length);
                            address+=2;
                            for (int k = 0; k < birthStatusGroups[i][j].length; k++) {
                                UNSAFE.putLong(address, birthStatusGroups[i][j][k]);
                                address+=8;
                            }
                        }
                    }
                }

                for (int i = 0; i < 90; i++) {
                    for (int j = 0; j < 9; j++) {
                        if (interesGroups[i][j].length != 0) {
                            long address = UNSAFE.allocateMemory(2 + 8*interesGroups[i][j].length);
                            size+=2 + 8*interesGroups[i][j].length;
                            interesGroupsIndex[i][j] = address;
                            UNSAFE.putShort(address, (short) interesGroups[i][j].length);
                            address+=2;
                            for (int k = 0; k < interesGroups[i][j].length; k++) {
                                UNSAFE.putLong(address, interesGroups[i][j][k]);
                                address+=8;
                            }
                        }
                    }
                }

                for (int i = 0; i < 70; i++) {
                    for (int j = 0; j < 9; j++) {
                        if (countryGroups[i][j].length != 0) {
                            long address = UNSAFE.allocateMemory(2 + 8*countryGroups[i][j].length);
                            size+=2 + 8*countryGroups[i][j].length;
                            countryGroupsIndex[i][j] = address;
                            UNSAFE.putShort(address, (short) countryGroups[i][j].length);
                            address+=2;
                            for (int k = 0; k < countryGroups[i][j].length; k++) {
                                UNSAFE.putLong(address, countryGroups[i][j][k]);
                                address+=8;
                            }
                        }
                    }
                }

                System.out.println("Finish AuxiliaryGroupsCalculatorTask, size=" + size);

            } catch (Exception ex) {
                ex.printStackTrace();
            }

            return Boolean.TRUE;
        }
    }


    private class GroupCalculator {

        final TIntIntHashMap[] groupCounts;
        final long[][] index;

        public GroupCalculator(long[][] index) {

            groupCounts = new TIntIntHashMap[9];
            for (int i = 0; i < 9; i++) {
                groupCounts[i] = new TIntIntHashMap();
            }
            this.index = index;
        }
        public void apply(Account account) {
            birthYear[account.id] = (byte)(BirthYearPredicate.calculateYear(account.birth) - 1900);
            joinedYear[account.id] = (byte)(JoinedYearPredicate.calculateYear(account.joined) - 2000);
            for (int j = 0; j < 9; j++) {
                processRecord3(account, groupCounts[j], masks[j]);
            }
        }

        public void complete() {
            Group[][] groupArrays = new Group[9][];
            Comparator[] comparators = new Comparator[9];
            for (int i = 0; i < 9; i++) {
                groupArrays[i] = new Group[groupCounts[i].size()];
                comparators[i] = new GroupsComparator(keys[i]);
            }
            long[][] index = this.index;
            for (int i = 0; i < 9; i++) {
                int counter = 0;
                for (int group : groupCounts[i].keys()) {
                    Group tmp = new Group();
                    tmp.count = groupCounts[i].get(group);
                    tmp.values = group;
                    groupArrays[i][counter++] = tmp;
                }
                Arrays.sort(groupArrays[i], comparators[i]);
                index[i] = new long[groupArrays[i].length];
                for (int j = 0; j < groupArrays[i].length; j++) {
                    Group grp = groupArrays[i][j];
                    long value = grp.count;
                    value |= ((long) grp.values << 32);
                    index[i][j] = value;
                }
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

        private class GroupsComparator implements Comparator<Group>{

            List<String> keys;

            GroupsComparator(List<String> keys) {
                this.keys = keys;
            }

            @Override
            public int compare(Group o1, Group o2) {
                return GroupCalculator.this.compare(o1.count, o1.values, o2.count, o2.values, keys,1);
            }
        }



    }




}
