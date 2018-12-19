package com.dgusev.hlcup2018.accountsapp.index;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class IndexHolder {

    public Map<String, ArrayList<Integer>> countryIndex;
    public List<Integer> notNullCountry;
    public List<Integer> nullCountry;
    public Map<String, ArrayList<Integer>> sexIndex;
    public Map<Integer, ArrayList<Integer>> statusIndex;
    public Map<String, ArrayList<Integer>> interestsIndex;

    public synchronized void init(List<AccountDTO> accountDTOList) {
        countryIndex = new HashMap<>();
        sexIndex = new HashMap<>();
        notNullCountry = new ArrayList<>();
        nullCountry = new ArrayList<>();
        statusIndex = new HashMap<>();
        sexIndex.put("m", new ArrayList<>());
        sexIndex.put("f", new ArrayList<>());
        for (int i = 0; i< 3; i++) {
            statusIndex.put(i, new ArrayList<>());
        }
        interestsIndex = new HashMap<>();
        for (AccountDTO accountDTO: accountDTOList) {
            if (accountDTO.country != null) {
                countryIndex.computeIfAbsent(accountDTO.country, k -> new ArrayList<>()).add(accountDTO.id);
                notNullCountry.add(accountDTO.id);
            } else {
                nullCountry.add(accountDTO.id);
            }
            sexIndex.get(accountDTO.sex).add(accountDTO.id);
            if (accountDTO.status.equals("свободны")) {
                statusIndex.get(0).add(accountDTO.id);
            } else if (accountDTO.status.equals("всё сложно")) {
                statusIndex.get(1).add(accountDTO.id);
            } else {
                statusIndex.get(2).add(accountDTO.id);
            }
            if (accountDTO.interests != null) {
                for (String interes: accountDTO.interests) {
                    interestsIndex.computeIfAbsent(interes, k -> new ArrayList<>()).add(accountDTO.id);
                }
            }
        }

        countryIndex.values().forEach(ArrayList::trimToSize);
    }

}
