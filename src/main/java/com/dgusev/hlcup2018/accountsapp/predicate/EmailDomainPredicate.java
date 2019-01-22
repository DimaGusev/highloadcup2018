package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.EmailDomainIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class EmailDomainPredicate extends AbstractPredicate {

    private String domain;
    private byte[] atDomain;

    public EmailDomainPredicate(String domain) {
        this.atDomain = ("@" + domain).getBytes();
        this.domain = domain;
    }

    @Override
    public boolean test(Account account) {
        return endsWith(account.email, atDomain);
    }

    private boolean endsWith(byte[] value, byte[] atDomain) {
        if (atDomain.length > value.length) {
            return false;
        }
        int index = value.length - atDomain.length;
        for (int i = 0; i < atDomain.length; i++) {
            if (atDomain[i] != value[index++]) {
                return false;
            }
        }
        return true;
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
