package com.dgusev.hlcup2018.accountsapp.model;

import java.util.List;

public class AccountDTO {

    public int id = -1;
    public String email;
    public String fname;
    public String sname;
    public String phone;
    public String sex;
    public int birth = Integer.MIN_VALUE;
    public String country;
    public String city;
    public int joined = Integer.MIN_VALUE;
    public String status;
    public String[] interests;
    public int premiumStart;
    public int premiumFinish;
    public long[] likes;
}
