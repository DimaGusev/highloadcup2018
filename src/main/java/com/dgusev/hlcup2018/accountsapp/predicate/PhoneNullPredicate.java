package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PhoneNullPredicate implements Predicate<Account> {

    private int nill;

    public PhoneNullPredicate(int nill) {
        this.nill = nill;
    }

    @Override
    public boolean test(Account Account) {
        if (nill == 0) {
            return Account.phone != null;
        } else  {
            return Account.phone == null;
        }
    }
}
