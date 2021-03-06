package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.index.JoinedYearIndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.function.Predicate;

public class JoinedYearPredicate extends AbstractPredicate {

    public static final int ORDER = 17;

    private static final int[] YEARS_ARRAY = new int[8];
    static {
        for (int i = 2010; i < 2018; i++) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.YEAR, i);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            YEARS_ARRAY[i - 2010] = (int)( calendar.getTime().getTime() / 1000);
        }
    }

    private int year;

    public JoinedYearPredicate setValue(int birth) {
        this.year = birth;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return IndexHolder.joinedYear[account.id] == year - 2000;
    }

    public static int calculateYear(int timestamp) {
        int result = Arrays.binarySearch(YEARS_ARRAY, timestamp);
        if (result >= 0) {
            return 2010 + result;
        } else {
            return 2010 - result - 2;
        }
    }

    public int getYear() {
        return year;
    }

    @Override
    public int getIndexCordiality() {
        return 186000;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new JoinedYearIndexScan(indexHolder, year);
    }

    @Override
    public double probability() {
        return 0.142857;
    }

    @Override
    public double cost() {
        return 1.1;
    }
}
