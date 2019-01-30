package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.index.BirthYearIndexScan;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.index.IndexScan;
import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.function.Predicate;

public class BirthYearPredicate extends AbstractPredicate {

    public static final int ORDER = 2;

    private static final int[] YEARS_ARRAY = new int[55];
    static {
        for (int i = 1950; i < 2005; i++) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.YEAR, i);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            YEARS_ARRAY[i - 1950] = (int)( calendar.getTime().getTime() / 1000);
        }
    }


    private int year;

    public BirthYearPredicate setValue(int birth) {
        this.year = birth;
        return this;
    }

    @Override
    public boolean test(Account account) {
        return IndexHolder.birthYear[account.id] == year - 1900;
    }

    public static int calculateYear(int timestamp) {
        int result = Arrays.binarySearch(YEARS_ARRAY, timestamp);
        if (result >= 0) {
            return 1950 + result;
        } else {
            return 1950 - result - 2;
        }
    }

    public int getYear() {
        return year;
    }

    @Override
    public int getIndexCordiality() {
        return 24000;
    }

    @Override
    public IndexScan createIndexScan(IndexHolder indexHolder) {
        return new BirthYearIndexScan(indexHolder, year);
    }

    @Override
    public double probability() {
        //return 0.018;
        return 0.0047;
    }

    @Override
    public double cost() {
        return 1.1;
    }
}
