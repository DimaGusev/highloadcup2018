package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.Group;
import com.dgusev.hlcup2018.accountsapp.service.ConvertorUtills;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
import io.netty.buffer.ByteBuf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class GroupFormatter {

    private static final byte[] COUNT = "{\"count\":".getBytes();

    private static final byte[] STATUS0 = "свободны".getBytes();
    private static final byte[] STATUS1 = "всё сложно".getBytes();
    private static final byte[] STATUS2 = "заняты".getBytes();


    private static final byte[] SEX = "sex".getBytes();
    private static final byte[] COUNTRY = "country".getBytes();
    private static final byte[] CITY = "city".getBytes();
    private static final byte[] STATUS = "status".getBytes();
    private static final byte[] INTERESTS = "interests".getBytes();

    @Autowired
    private Dictionary dictionary;

    public int format(Group group, List<String> keys, byte[] responseBuf, int startIndex) {
        int index = startIndex;
        System.arraycopy(COUNT, 0, responseBuf, index, COUNT.length);
        index+=COUNT.length;
        index = encodeLong(responseBuf, index, group.count);
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            if (key.equals("sex")) {
                index = writeField(responseBuf, index, false, SEX);
                index = writeSex(responseBuf, index, (group.values & 0b00000001) == 1);
            } else if (key.equals("status")) {
                index = writeField(responseBuf, index, false, STATUS);
                index = writeStatus(responseBuf, index, (byte)((group.values >> 1) & 0b00000011));
            } else if (key.equals("interests")) {
                byte[] interes = dictionary.getInteresBytes((byte)((group.values >> 20) & 0b01111111));
                if (interes != null) {
                    index = writeField(responseBuf, index, false, INTERESTS);
                    index = writeStringValue(responseBuf, index, interes);
                }
            } else if (key.equals("country")) {
                byte[] country = dictionary.getCountryBytes((byte)((group.values >> 3) & 0b01111111));
                if (country != null) {
                    index = writeField(responseBuf, index, false, COUNTRY);
                    index = writeStringValue(responseBuf, index, country);
                }
            } else if (key.equals("city")) {
                byte[] city = dictionary.getCityBytes((int)((group.values >> 10) & 0b0000001111111111));
                if (city != null) {
                    index = writeField(responseBuf, index, false, CITY);
                    index = writeStringValue(responseBuf, index, city);
                }
            }
        }
        responseBuf[index++] = '}';
        return index;
    }


    private int writeStringValue(byte[] arr, int index, byte[] value) {
        arr[index++] = '\"';
        System.arraycopy(value, 0, arr, index, value.length);
        index+=value.length;
        arr[index++] = '\"';
        return index;
    }

    private int writeSex(byte[] arr, int index, boolean sex) {
        arr[index++] = '\"';
        if (sex) {
            arr[index++] = 'm';
        } else {
            arr[index++] = 'f';
        }
        arr[index++] = '\"';
        return index;
    }

    private int writeStatus(byte[] arr, int index, byte status) {
        arr[index++] = '\"';
        if (status == 0) {
            System.arraycopy(STATUS0, 0, arr, index, STATUS0.length);
            index+=STATUS0.length;
        } else if (status == 1){
            System.arraycopy(STATUS1, 0, arr, index, STATUS1.length);
            index+=STATUS1.length;
        } else {
            System.arraycopy(STATUS2, 0, arr, index, STATUS2.length);
            index+=STATUS2.length;
        }
        arr[index++] = '\"';
        return index;
    }

    private int writeField(byte[] arr, int index, boolean first, byte[] field) {
        if (!first) {
            arr[index++] = ',';
        }
        arr[index++] = '\"';
        System.arraycopy(field, 0, arr, index, field.length);
        index+=field.length;
        arr[index++] = '\"';
        arr[index++] = ':';
        return index;
    }

    private static final int POW10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};


    private int writeField(byte[] arr, int index, boolean first, String field) {
        if (!first) {
            arr[index++] = ',';
        }
        arr[index++] = '\"';
        for (int i = 0; i < field.length(); i++) {
            arr[index++] = (byte) field.charAt(i);
        }
        arr[index++] = '\"';
        arr[index++] = ':';
        return index;
    }



    private int writeStringValue(byte[] arr, int index, String value) {
        arr[index++] = '\"';
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch < 0x7f) {
                arr[index++] =(byte)ch;
            } else {
                arr[index++] =(byte) ((ch >> 6) | 0xC0);
                arr[index++] =(byte) ((ch & 0x3F) | 0x80);
            }
        }
        arr[index++] = '\"';
        return index;
    }

    public static int encodeLong(byte[] arr, int index, long value) {
        if (value < 0) {
            arr[index++] = 45;
            value = -value;
        }
        boolean printZero = false;
        for (int i = 9; i>=0; i--) {
            int digit = (int)(value/POW10[i]);
            if (digit == 0 && !printZero) {
                continue;
            }
            arr[index++] = (byte)(48 + digit);
            printZero=true;
            value -= (value/POW10[i]) * POW10[i];
        }
        return index;
    }
}
