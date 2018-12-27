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

    public static byte convertStatusNumber(String status) {
        if (status.equals("свободны")) {
            return 0;
        } else if (status.equals("всё сложно")) {
            return 1;
        } else if (status.equals("заняты")){
            return 2;
        } else  {
            throw new BadRequest();
        }
    }

    public static String convertStatusNumber(byte status) {
        if (status == 0) {
            return "свободны";
        } else if (status == 1) {
            return "всё сложно";
        } else {
            return "заняты";
        }
    }
}
