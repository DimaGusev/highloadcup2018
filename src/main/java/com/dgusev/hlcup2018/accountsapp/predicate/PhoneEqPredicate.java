package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PhoneEqPredicate implements Predicate<Account> {

    private String phone;

    public PhoneEqPredicate(String phone) {
        this.phone = phone;
    }

    @Override
    public boolean test(Account account) {
        return account.phone != null && account.phone.equals(phone);
    }

    public String getPhone() {
        return phone;
    }
}
