package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;

import java.util.List;
import java.util.function.Predicate;

public class LikesContainsPredicate implements Predicate<AccountDTO> {

    private int[] likes;

    public LikesContainsPredicate(int[] interests) {
        PredicateStatistics.lc.incrementAndGet();
        this.likes = interests;
    }

    @Override
    public boolean test(AccountDTO accountDTO) {
        if (accountDTO.likes != null && !accountDTO.likes.isEmpty()) {
            for (int i = 0; i< likes.length; i++) {
                int like = likes[i];
                if (!containsLike(accountDTO.likes, like)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean containsLike(List<AccountDTO.Like> likes, int like) {
        for (int i = 0; i < likes.size(); i++) {
            AccountDTO.Like l = likes.get(i);
            if (l.id == like) {
                return true;
            }
        }
        return false;
    }
}
