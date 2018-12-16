package com.dgusev.hlcup2018.accountsapp.init;

import org.springframework.stereotype.Component;

@Component
public class NowProvider {

    private int now;

    public int getNow() {
        return now;
    }

    public void initNow(int now) {
        this.now = now;
    }
}
