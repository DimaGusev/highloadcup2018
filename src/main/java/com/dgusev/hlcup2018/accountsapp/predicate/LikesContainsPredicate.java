package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.LikesContainsIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.List;
import java.util.function.Predicate;

public class LikesContainsPredicate extends AbstractPredicate {

    public static final int ORDER = 18;

    private int[] likes;

    public LikesContainsPredicate setValue(int[] interests) {
        this.likes = interests;
        return this;
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
            int id = (int)(l >> 32);
            if (id == like) {
                return true;
            }
            if (id < like) {
                return false;
            }
        }
        return false;
    }

    public int[] getLikes() {
        return likes;
    }

    @Override
    public int getIndexCordiality() {
        return 50 * likes.length;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new LikesContainsIndexScan(indexHolder, likes);
    }

    @Override
    public double probability() {
        return 0.00005/likes.length;
    }

    @Override
    public double cost() {
        return 1.5;
    }
}
