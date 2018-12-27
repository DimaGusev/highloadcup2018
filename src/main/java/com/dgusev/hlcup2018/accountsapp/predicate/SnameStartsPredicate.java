package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
import gnu.trove.impl.Constants;

import java.util.function.Predicate;

public class SnameStartsPredicate implements Predicate<Account> {

    private String start;
    private Dictionary dictionary;

    public SnameStartsPredicate(String start, Dictionary dictionary) {
        this.start = start;
        this.dictionary = dictionary;
    }

    @Override
    public boolean test(Account account) {
        return account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE && dictionary.getSname(account.sname).startsWith(start);
    }
}
