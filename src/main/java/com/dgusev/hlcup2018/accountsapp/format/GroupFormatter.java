package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.Group;
import io.netty.buffer.ByteBuf;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class GroupFormatter {

    private static final byte[] COUNT = "{\"count\":".getBytes();
    private static final byte[] NULL = "null".getBytes();

    public void format(Group group, List<String> keys, ByteBuf responseBuf, byte[] arr) {
        int index = 0;
        System.arraycopy(COUNT, 0, arr, 0, COUNT.length);
        index+=COUNT.length;
        index = encodeLong(arr, index, group.count, responseBuf);
        for (int i = 0; i < group.values.size() && i < keys.size(); i++) {
                    if (group.values.get(i) != null) {
                        index = writeField(arr, index, false, keys.get(i));
                        index = writeStringValue(arr, index, group.values.get(i));
                    }
        }
        arr[index++] = '}';
        responseBuf.writeBytes(arr, 0, index);
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
