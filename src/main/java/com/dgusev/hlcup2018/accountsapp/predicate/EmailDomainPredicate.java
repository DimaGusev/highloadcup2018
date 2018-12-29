package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailDomainPredicate implements Predicate<Account> {

    private String domain;
    private String atDomain;

    public EmailDomainPredicate(String domain) {
        this.atDomain = "@" + domain;
        this.domain = domain;
    }

    @Override
    public boolean test(Account Account) {
        return Account.email.endsWith(atDomain);
    }

    public String getDomain() {
        return domain;
    }
}
