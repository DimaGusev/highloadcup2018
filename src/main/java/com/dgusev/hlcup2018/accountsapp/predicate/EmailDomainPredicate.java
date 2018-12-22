package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class EmailDomainPredicate implements Predicate<AccountDTO> {

    private String domain;

    public EmailDomainPredicate(String domain) {
        PredicateStatistics.ed.incrementAndGet();
        this.domain = "@" + domain;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        return accountDTO.email.endsWith(domain);
    }
}
