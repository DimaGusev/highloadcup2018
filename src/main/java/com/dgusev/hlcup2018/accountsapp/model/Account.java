package com.dgusev.hlcup2018.accountsapp.model;

public class Account {
    public int id = -1;
    public byte[] email;
    public int fname;
    public int sname;
    public byte[] phone;
    public boolean sex;
    public int birth = Integer.MIN_VALUE;
    public byte country;
    public int city;
    public int joined = Integer.MIN_VALUE;
    public byte status;
    public byte[] interests;
    public int premiumStart;
    public int premiumFinish;
    public long[] likes;
    public boolean premium;
}
