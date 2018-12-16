package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.List;
import java.util.function.Predicate;

public class LikesContainsPredicate implements Predicate<AccountDTO> {

    private List<Integer> likes;

    public LikesContainsPredicate(List<Integer> interests) {
        this.likes = interests;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (accountDTO.likes != null && !accountDTO.likes.isEmpty()) {
            for (Integer like: likes) {
                if (!containsLike(accountDTO.likes, like)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean containsLike(List<AccountDTO.Like> likes, Integer like) {
        for (AccountDTO.Like l : likes) {
            if (l.id == like) {
                return true;
            }
        }
        return false;
    }
}
