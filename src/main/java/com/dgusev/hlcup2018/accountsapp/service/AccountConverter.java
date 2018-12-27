package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import gnu.trove.impl.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AccountConverter {

    @Autowired
    private Dictionary dictionary;

    public Account convert(AccountDTO accountDTO) {
        Account account = new Account();
        account.id = accountDTO.id;
        account.email = accountDTO.email;
        account.fname = accountDTO.fname;
        account.sname = accountDTO.sname;
        account.phone = accountDTO.phone;

        account.sex = ConvertorUtills.convertSex(accountDTO.sex);
        account.birth = accountDTO.birth;
        if (accountDTO.country != null) {
            account.country = dictionary.getOrCreateCountry(accountDTO.country);
        } else  {
            account.country = Constants.DEFAULT_BYTE_NO_ENTRY_VALUE;
        }
        account.city = accountDTO.city;
        account.joined = accountDTO.joined;
        account.status = ConvertorUtills.convertStatusNumber(accountDTO.status);
        account.interests = accountDTO.interests;
        account.premiumStart = accountDTO.premiumStart;
        account.premiumFinish = accountDTO.premiumFinish;
        account.likes = accountDTO.likes;
        return account;
    }


}
