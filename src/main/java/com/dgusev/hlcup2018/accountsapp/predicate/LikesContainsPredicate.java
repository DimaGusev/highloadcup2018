package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.List;
import java.util.function.Predicate;

public class LikesContainsPredicate implements Predicate<Account> {

    private int[] likes;

    public LikesContainsPredicate(int[] interests) {
        this.likes = interests;
    }

    @Override
    public boolean test(Account Account) {
        if (Account.likes != null && Account.likes.length != 0) {
            for (int i = 0; i< likes.length; i++) {
                int like = likes[i];
                if (!containsLike(Account.likes, like)) {
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
            if ((int)(l >> 32)== like) {
                return true;
            }
        }
        return false;
    }

    public int[] getLikes() {
        return likes;
    }
}
