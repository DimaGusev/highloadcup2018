package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import gnu.trove.impl.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class AccountConverter {

    @Autowired
    private NowProvider nowProvider;

    @Autowired
    private Dictionary dictionary;

    public synchronized Account convert(AccountDTO accountDTO) {
        Account account = new Account();
        account.id = accountDTO.id;
        account.email = accountDTO.email;
        account.sex = ConvertorUtills.convertSex(accountDTO.sex);
        if (accountDTO.fname != null) {
            account.fname = dictionary.getOrCreateFname(accountDTO.fname);
            dictionary.updateFnameSexDictionary(account.fname, account.sex);
        } else {
            account.fname = Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        }
        if (accountDTO.sname != null) {
            account.sname = dictionary.getOrCreateSname(accountDTO.sname);
        } else {
            account.sname = Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        }
        account.phone = accountDTO.phone;


        account.birth = accountDTO.birth;
        if (accountDTO.country != null) {
            account.country = dictionary.getOrCreateCountry(accountDTO.country);
        } else  {
            account.country = Constants.DEFAULT_BYTE_NO_ENTRY_VALUE;
        }
        if (accountDTO.city != null) {
            account.city = dictionary.getOrCreateCity(accountDTO.city);
        } else  {
            account.city = Constants.DEFAULT_INT_NO_ENTRY_VALUE;
        }
        account.joined = accountDTO.joined;
        account.status = ConvertorUtills.convertStatusNumber(accountDTO.status);
        if (accountDTO.interests != null) {
            byte[] interests = new byte[accountDTO.interests.length];
            for (int i = 0; i < accountDTO.interests.length; i++) {
                interests[i] = dictionary.getOrCreateInteres(accountDTO.interests[i]);
            }
            account.interests = interests;
            Arrays.sort(account.interests);
        }
        account.premiumStart = accountDTO.premiumStart;
        account.premiumFinish = accountDTO.premiumFinish;
        account.premium = account.premiumStart != 0 && account.premiumStart <= nowProvider.getNow() && (account.premiumFinish == 0 || account.premiumFinish > nowProvider.getNow());
        account.likes = accountDTO.likes;
        return account;
    }


}
