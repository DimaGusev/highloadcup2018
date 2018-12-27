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
        if (accountDTO.likes != null && accountDTO.likes.length != 0) {
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

    private boolean containsLike(long[] likes, int like) {
        for (int i = 0; i < likes.length; i++) {
            long l = likes[i];
            if ((int)(l & 0x0000ffff)== like) {
                return true;
            }
        }
        return false;
    }
}
