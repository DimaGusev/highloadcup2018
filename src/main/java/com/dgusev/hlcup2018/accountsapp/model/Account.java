package com.dgusev.hlcup2018.accountsapp.model;

import java.util.List;

public class Account {
    public int id = -1;
    public String email;
    public int fname = -1;
    public int sname = -1;
    public String phone;
    public boolean sex;
    public int birth = Integer.MIN_VALUE;
    public int country = -1;
    public int city = -1;
    public int joined = Integer.MIN_VALUE;
    public int status = -1;
    public int[] interests;
    public int premiumStart;
    public int premiumFinish;
}
