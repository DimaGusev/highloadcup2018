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
    private static final byte[] NULL = "null".getBytes();

    @Autowired
    private Dictionary dictionary;

    public void format(Group group, List<String> keys, ByteBuf responseBuf, byte[] arr) {
        int index = 0;
        System.arraycopy(COUNT, 0, arr, 0, COUNT.length);
        index+=COUNT.length;
        index = encodeLong(arr, index, group.count, responseBuf);
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            if (key.equals("sex")) {
                index = writeField(arr, index, false, keys.get(i));
                index = writeStringValue(arr, index, ConvertorUtills.convertSex((group.values & 0b00000001) == 1));
            } else if (key.equals("status")) {
                index = writeField(arr, index, false, keys.get(i));
                index = writeStringValue(arr, index, ConvertorUtills.convertStatusNumber((byte)((group.values >> 1) & 0b00000011)));
            } else if (key.equals("interests")) {
                String interes = dictionary.getInteres((byte)((group.values >> 20) & 0b01111111));
                if (interes != null) {
                    index = writeField(arr, index, false, keys.get(i));
                    index = writeStringValue(arr, index, interes);
                }
            } else if (key.equals("country")) {
                String country = dictionary.getCountry((byte)((group.values >> 3) & 0b01111111));
                if (country != null) {
                    index = writeField(arr, index, false, keys.get(i));
                    index = writeStringValue(arr, index, country);
                }
            } else if (key.equals("city")) {
                String city = dictionary.getCity((int)((group.values >> 10) & 0b0000001111111111));
                if (city != null) {
                    index = writeField(arr, index, false, keys.get(i));
                    index = writeStringValue(arr, index, city);
                }
            }
        }
        arr[index++] = '}';
        responseBuf.writeBytes(arr, 0, index);
    }

    private List<String> getGroupValues(int groupDefinition, List<String> keys) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            if (key.equals("sex")) {
                result.add(ConvertorUtills.convertSex((groupDefinition & 0b00000001) == 1));
            } else if (key.equals("status")) {
                result.add(ConvertorUtills.convertStatusNumber((byte)((groupDefinition >> 1) & 0b00000011)));
            } else if (key.equals("interests")) {
                result.add(dictionary.getInteres((byte)((groupDefinition >> 20) & 0b01111111)));
            } else if (key.equals("country")) {
                result.add(dictionary.getCountry((byte)((groupDefinition >> 3) & 0b01111111)));
            } else if (key.equals("city")) {
                result.add(dictionary.getCity((int)((groupDefinition >> 10) & 0b0000001111111111)));
            }
        }
        return result;
    }

    private static final int POW10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    public static void encodeLong(long value, ByteBuf responseBuf) {
        if (value < 0) {
            responseBuf.writeByte((byte)45);
            value = -value;
        }
        boolean printZero = false;
        for (int i = 9; i>=0; i--) {
            int digit = (int)(value/POW10[i]);
            if (digit == 0 && !printZero) {
                continue;
            }
            responseBuf.writeByte((byte)(48 + digit));
            printZero=true;
            value -= (value/POW10[i]) * POW10[i];
        }
    }
    private void writeField(ByteBuf responseBuf, boolean first, String field) {
        if (!first) {
            responseBuf.writeByte(',');
        }
        responseBuf.writeByte('\"');
        for (int i = 0; i < field.length(); i++) {
            responseBuf.writeByte(field.charAt(i));
        }
        responseBuf.writeByte('\"');
        responseBuf.writeByte(':');
    }

    private void writeStringValue(ByteBuf responseBuf, String value) {
        responseBuf.writeByte('\"');
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch < 0x7f) {
                responseBuf.writeByte((byte)ch);
            } else {
                responseBuf.writeByte( (byte) ((ch >> 6) | 0xC0));
                responseBuf.writeByte( (byte) ((ch & 0x3F) | 0x80));
            }
        }
        responseBuf.writeByte('\"');
    }

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

    public static int encodeLong(byte[] arr, int index, long value, ByteBuf responseBuf) {
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
