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
    public Map<String, ArrayList<Integer>> sexIndex;

    public void init(List<AccountDTO> accountDTOList) {
        countryIndex = new HashMap<>();
        sexIndex = new HashMap<>();
        sexIndex.put("m", new ArrayList<>());
        sexIndex.put("f", new ArrayList<>());
        for (AccountDTO accountDTO: accountDTOList) {
            if (accountDTO.country != null) {
                if (countryIndex.get(accountDTO.country) == null) {
                    countryIndex.put(accountDTO.country, new ArrayList<>());
                }
                countryIndex.get(accountDTO.country).add(accountDTO.id);
            }
            sexIndex.get(accountDTO.sex).add(accountDTO.id);
        }

        countryIndex.values().forEach(ArrayList::trimToSize);
    }

}
