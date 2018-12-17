package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class PhoneCodePredicate implements Predicate<AccountDTO> {

    private String code;

    public PhoneCodePredicate(String code) {
        PredicateStatistics.pc.incrementAndGet();
        this.code = code;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.phone != null && accountDTO.phone.contains("(" + code + ")");
    }
}
