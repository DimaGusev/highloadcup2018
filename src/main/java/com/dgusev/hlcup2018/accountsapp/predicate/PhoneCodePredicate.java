package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.function.Predicate;

public class PhoneCodePredicate implements Predicate<AccountDTO> {

    private String code;

    public PhoneCodePredicate(String code) {
        PredicateStatistics.pc.incrementAndGet();
        this.code = code;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (accountDTO.phone == null) {
            return false;
        }
        int open = accountDTO.phone.indexOf("(");
        if (open == -1) {
            return false;
        }
        if (open + code.length() > accountDTO.phone.length()) {
            return false;
        }
        if (accountDTO.phone.charAt(open + code.length() + 1) != ')') {
            return false;
        }
        for (int i = 0; i < code.length(); i++) {
            if (accountDTO.phone.charAt(open + i + 1) != code.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
