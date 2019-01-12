package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.List;
import java.util.function.Predicate;

public class InterestsContainsPredicate implements Predicate<Account> {

    private byte[] interests;

    public InterestsContainsPredicate(byte[] interests) {
        this.interests = interests;
    }

    @Override
    public boolean test(Account account) {
        if (account.interests != null && account.interests.length != 0) {
            for (int i = 0; i < interests.length; i++) {
                if (!contains(account.interests, interests[i])) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public byte[] getInterests() {
        return interests;
    }

    private boolean contains(byte[] arrray, byte element) {
        for (int i = 0; i < arrray.length; i++) {
            if (arrray[i] == element) {
                return true;
            }
            if (arrray[i] > element) {
                return false;
            }
        }
        return false;
    }
}
