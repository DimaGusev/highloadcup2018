package com.dgusev.hlcup2018.accountsapp.model;


public class Account {
    public int id = -1;
    public String email;
    public int fname;
    public String sname;
    public String phone;
    public boolean sex;
    public int birth = Integer.MIN_VALUE;
    public byte country;
    public int city;
    public int joined = Integer.MIN_VALUE;
    public byte status;
    public String[] interests;
    public int premiumStart;
    public int premiumFinish;
    public long[] likes;
}
