package com.dgusev.hlcup2018.accountsapp.model;

public class NotFoundRequest extends RuntimeException {
    public static final NotFoundRequest INSTANCE = new NotFoundRequest();
}
