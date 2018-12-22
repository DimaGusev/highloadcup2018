package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.List;
import java.util.function.Predicate;

public class InterestsContainsPredicate implements Predicate<AccountDTO> {

    private List<String> interests;

    public InterestsContainsPredicate(List<String> interests) {
        PredicateStatistics.ic.incrementAndGet();
        this.interests = interests;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (accountDTO.interests != null && !accountDTO.interests.isEmpty()) {
            for (int i = 0; i < interests.size(); i++) {
                String interes = interests.get(i);
                if (!accountDTO.interests.contains(interes)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public List<String> getInterests() {
        return interests;
    }
}
