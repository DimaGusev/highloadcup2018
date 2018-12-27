package com.dgusev.hlcup2018.accountsapp.predicate;

import com.dgusev.hlcup2018.accountsapp.model.Account;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.function.Predicate;

public class JoinedYearPredicate implements Predicate<Account> {

    private static final int[] YEARS_ARRAY = new int[8];
    static {
        for (int i = 2010; i < 2018; i++) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.YEAR, i);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 0);
            calendar.set(Calendar.HOUR, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            YEARS_ARRAY[i - 2010] = (int)( calendar.getTime().getTime() / 1000);
        }
    }

    private int year;

    public JoinedYearPredicate(int birth) {
        PredicateStatistics.jy.incrementAndGet();
        this.year = birth;
    }

    @Override
    public boolean test(Account Account) {
        return calculateYear(Account.joined) == year;
    }

    private static int calculateYear(int timestamp) {
        int result = Arrays.binarySearch(YEARS_ARRAY, timestamp);
        if (result >= 0) {
            return 2010 + result;
        } else {
            return 2010 - result - 2;
        }
    }
}
