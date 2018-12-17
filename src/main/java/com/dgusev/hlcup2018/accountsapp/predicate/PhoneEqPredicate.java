package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class PhoneEqPredicate implements Predicate<AccountDTO> {

    private String phone;

    public PhoneEqPredicate(String phone) {
        PredicateStatistics.pe.incrementAndGet();
        this.phone = phone;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.phone != null && accountDTO.phone.equals(phone);
    }
}
