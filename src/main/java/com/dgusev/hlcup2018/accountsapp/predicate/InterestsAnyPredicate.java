package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.List;
import java.util.function.Predicate;

public class InterestsAnyPredicate implements Predicate<AccountDTO> {

    private String[] interests;

    public InterestsAnyPredicate(List<String> interests) {
        PredicateStatistics.ia.incrementAndGet();
        this.interests = interests.toArray(new String[interests.size()]);
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (accountDTO.interests != null && accountDTO.interests.length != 0) {
            for (int i = 0; i < interests.length; i++) {
                String interes = interests[i];
                if (contains(accountDTO.interests, interes)) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    private boolean contains(String[] arrray, String element) {
        for (int i = 0; i < arrray.length; i++) {
            if (arrray[i].equals(element)) {
                return true;
            }
        }
        return false;
    }

    public String[] getInterests() {
        return interests;
    }
}
