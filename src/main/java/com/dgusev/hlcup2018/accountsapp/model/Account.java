package com.dgusev.hlcup2018.accountsapp.model;


public class Account {
    public int id = -1;
    public String email;
    public String fname;
    public String sname;
    public String phone;
    public boolean sex;
    public int birth = Integer.MIN_VALUE;
    public String country;
    public String city;
    public int joined = Integer.MIN_VALUE;
    public byte status;
    public String[] interests;
    public int premiumStart;
    public int premiumFinish;
    public long[] likes;
}
