package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.List;
import java.util.function.Predicate;

public class FnameAnyPredicate implements Predicate<Account> {

    private List<String> fnames;

    public FnameAnyPredicate(List<String> fnames) {
        PredicateStatistics.fa.incrementAndGet();
        this.fnames = fnames;
    }

    @Override
    public boolean test(Account Account) {
        return Account.fname != null && fnames.contains(Account.fname);
    }
}
