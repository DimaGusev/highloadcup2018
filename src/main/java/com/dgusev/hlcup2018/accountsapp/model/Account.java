package com.dgusev.hlcup2018.accountsapp.model;


import com.dgusev.hlcup2018.accountsapp.service.Unsafe;

public class Account {
    public int id = -1;
    public String email;
    public int fname;
    public int sname;
    public String phone;
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
   /* public int likesCount;
    public long likeAddress;

    public int likeCount() {
        return Unsafe.UNSAFE.getByte(likeAddress);
    }

    public int getLikes(long[] likes) {
        int count = Unsafe.UNSAFE.getByte(likeAddress);
        if (count == 0) {
            return 0;
        }
        long position = likeAddress + 1;
        for (int i = 0; i < count; i++) {
            likes[i] = Unsafe.UNSAFE.getLong(position);
            position+=8;
        }
        return count;
    } */
}
