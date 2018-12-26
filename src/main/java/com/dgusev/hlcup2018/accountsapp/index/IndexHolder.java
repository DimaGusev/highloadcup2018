package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class IndexHolder {

    public Map<String, int[]> countryIndex;
    public Map<String, int[]> sexIndex;
    public Map<Integer, int[]> statusIndex;
    public Map<String, int[]> interestsIndex;
    public Map<String, int[]> cityIndex;
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

    public synchronized void init(AccountDTO[] accountDTOList, int size) {
        int now = nowProvider.getNow();
        Map<String, List<Integer>> tmpCountryIndex = new HashMap<>();
        Map<String, List<Integer>> tmpSexIndex = new HashMap<>();
        Map<String, List<Integer>> tmpEmailDomainIndex = new HashMap<>();
        List<Integer> tmpNotNullCountry = new ArrayList<>();
        List<Integer> tmpNullCountry = new ArrayList<>();
        List<Integer> tmpNotNullCity = new ArrayList<>();
        List<Integer> tmpNullCity = new ArrayList<>();
        Map<Integer, List<Integer>> tmpStatusIndex = new HashMap<>();
        Map<Integer, List<Integer>> tmpBirthYearIndex = new HashMap<>();
        tmpSexIndex.put("m", new ArrayList<>());
        tmpSexIndex.put("f", new ArrayList<>());
        for (int i = 0; i< 3; i++) {
            tmpStatusIndex.put(i, new ArrayList<>());
        }
        Map<String, List<Integer>> tmpInterestsIndex = new HashMap<>();
        Map<String, List<Integer>> tmpCityIndex = new HashMap<>();
        Map<Integer, Set<Integer>> tmpLikesIndex = new HashMap<>();
        List<Integer> tmpPremiumIndex = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            AccountDTO accountDTO = accountDTOList[i];
            if (accountDTO.country != null) {
                tmpCountryIndex.computeIfAbsent(accountDTO.country, k -> new ArrayList<>()).add(accountDTO.id);
                tmpNotNullCountry.add(accountDTO.id);
            } else {
                tmpNullCountry.add(accountDTO.id);
            }
            if (accountDTO.city != null) {
                tmpCityIndex.computeIfAbsent(accountDTO.city, k -> new ArrayList<>()).add(accountDTO.id);
                tmpNotNullCity.add(accountDTO.id);
            } else {
                tmpNullCity.add(accountDTO.id);
            }
            tmpSexIndex.get(accountDTO.sex).add(accountDTO.id);
            if (accountDTO.status.equals("свободны")) {
                tmpStatusIndex.get(0).add(accountDTO.id);
            } else if (accountDTO.status.equals("всё сложно")) {
                tmpStatusIndex.get(1).add(accountDTO.id);
            } else {
                tmpStatusIndex.get(2).add(accountDTO.id);
            }
            if (accountDTO.interests != null) {
                for (String interes: accountDTO.interests) {
                    tmpInterestsIndex.computeIfAbsent(interes, k -> new ArrayList<>()).add(accountDTO.id);
                }
            }
            int year = new Date(accountDTO.birth * 1000L).getYear() + 1900;
            tmpBirthYearIndex.computeIfAbsent(year, k -> new ArrayList<>()).add(accountDTO.id);
            if (accountDTO.premiumStart != 0 && accountDTO.premiumStart <= now && (accountDTO.premiumFinish == 0 || accountDTO.premiumFinish > now)) {
                tmpPremiumIndex.add(accountDTO.id);
            }
            int at = accountDTO.email.lastIndexOf('@');
            String domain = accountDTO.email.substring(at + 1);
            tmpEmailDomainIndex.computeIfAbsent(domain, k -> new ArrayList<>()).add(accountDTO.id);
            if (accountDTO.likes != null && accountDTO.likes.length != 0) {
                for (AccountDTO.Like like: accountDTO.likes) {
                    if (!tmpLikesIndex.containsKey(like.id)) {
                        tmpLikesIndex.put(like.id, new LinkedHashSet<>());
                    }
                    tmpLikesIndex.get(like.id).add(accountDTO.id);
                }
            }
        }
        countryIndex = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry: tmpCountryIndex.entrySet()) {
            countryIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        sexIndex = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry: tmpSexIndex.entrySet()) {
            sexIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        statusIndex = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry: tmpStatusIndex.entrySet()) {
            statusIndex.put(entry.getKey(), entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .toArray());
        }
        interestsIndex = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry: tmpInterestsIndex.entrySet()) {
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
        cityIndex = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry: tmpCityIndex.entrySet()) {
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
