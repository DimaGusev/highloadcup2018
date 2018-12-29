package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.function.Predicate;

public class PhoneCodePredicate implements Predicate<Account> {

    private String code;

    public PhoneCodePredicate(String code) {
        this.code = code;
    }

    @Override
    public boolean test(Account Account) {
        if (Account.phone == null) {
            return false;
        }
        int open = Account.phone.indexOf("(");
        if (open == -1) {
            return false;
        }
        if (open + code.length() > Account.phone.length()) {
            return false;
        }
        if (Account.phone.charAt(open + code.length() + 1) != ')') {
            return false;
        }
        for (int i = 0; i < code.length(); i++) {
            if (Account.phone.charAt(open + i + 1) != code.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
