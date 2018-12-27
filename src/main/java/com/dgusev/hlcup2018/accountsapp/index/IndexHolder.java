package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import gnu.trove.impl.Constants;
import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class IndexHolder {

    public TByteObjectMap<int[]> countryIndex;
    public TByteObjectMap<int[]> sexIndex;
    public TByteObjectMap<int[]> statusIndex;
    public TByteObjectMap<int[]> interestsIndex;
    public TIntObjectMap<int[]> cityIndex;
    public Map<Integer, int[]> birthYearIndex;
    public Map<String, int[]> emailDomainIndex;
    public int[] notNullCountry;
    public int[] nullCountry;
    public int[] notNullCity;
    public int[] nullCity;
    public int[] premiumIndex;
    public Map<Integer, int[]> likesIndex;

    @Autowired
    private NowProvider nowProvider;

    public synchronized void init(Account[] accountDTOList, int size) {
        int now = nowProvider.getNow();
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
        for (int i = 0; i< 3; i++) {
            tmpStatusIndex.put((byte)i, new ArrayList<>());
        }
        Map<Byte, List<Integer>> tmpInterestsIndex = new HashMap<>();
        Map<Integer, List<Integer>> tmpCityIndex = new HashMap<>();
        Map<Integer, Set<Integer>> tmpLikesIndex = new HashMap<>();
        List<Integer> tmpPremiumIndex = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Account account= accountDTOList[i];
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
                for (byte interes: account.interests) {
                    tmpInterestsIndex.computeIfAbsent(interes, k -> new ArrayList<>()).add(account.id);
                }
            }
            int year = new Date(account.birth * 1000L).getYear() + 1900;
            tmpBirthYearIndex.computeIfAbsent(year, k -> new ArrayList<>()).add(account.id);
            if (account.premiumStart != 0 && account.premiumStart <= now && (account.premiumFinish == 0 || account.premiumFinish > now)) {
                tmpPremiumIndex.add(account.id);
            }
            int at = account.email.lastIndexOf('@');
            String domain = account.email.substring(at + 1);
            tmpEmailDomainIndex.computeIfAbsent(domain, k -> new ArrayList<>()).add(account.id);
            if (account.likes != null && account.likes.length != 0) {
                for (long like: account.likes) {
                    int id = (int)(like & 0x0000ffff);
                    if (!tmpLikesIndex.containsKey(id)) {
                        tmpLikesIndex.put(id, new LinkedHashSet<>());
                    }
                    tmpLikesIndex.get(id).add(account.id);
                }
            }
        }
        countryIndex = new TByteObjectHashMap<>();
        for (Map.Entry<Byte, List<Integer>> entry: tmpCountryIndex.entrySet()) {
            countryIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        sexIndex = new TByteObjectHashMap<>();
        for (Map.Entry<Boolean, List<Integer>> entry: tmpSexIndex.entrySet()) {
            sexIndex.put(entry.getKey() ? (byte)1 : 0, entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        statusIndex = new TByteObjectHashMap<>();
        for (Map.Entry<Byte, List<Integer>> entry: tmpStatusIndex.entrySet()) {
            statusIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        interestsIndex = new TByteObjectHashMap<>();
        for (Map.Entry<Byte, List<Integer>> entry: tmpInterestsIndex.entrySet()) {
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
        for (Map.Entry<Integer, List<Integer>> entry: tmpCityIndex.entrySet()) {
            cityIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        birthYearIndex = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry: tmpBirthYearIndex.entrySet()) {
            birthYearIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        premiumIndex = tmpPremiumIndex.stream()
                .mapToInt(Integer::intValue)
                .toArray();
        emailDomainIndex = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry: tmpEmailDomainIndex.entrySet()) {
            emailDomainIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        likesIndex = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry: tmpLikesIndex.entrySet()) {
            likesIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
    }

}
