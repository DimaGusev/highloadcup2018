package com.dgusev.hlcup2018.accountsapp.service;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;

public class ConvertorUtills {

    public static boolean convertSex(String sex) {
            if (sex.equals("m")) {
                return true;
            } else if (sex.equals("f")) {
                return false;
            } else {
                throw new BadRequest();
            }
    }

    public static String convertSex(boolean sex) {
        if (sex) {
            return "m";
        } else {
            return "f";
        }
    }
}
