package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.EmailDomainIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailDomainPredicate extends AbstractPredicate {

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

    @Override
    public int getIndexCordiality() {
        return 100000;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new EmailDomainIndexScan(indexHolder, domain);
    }
}
