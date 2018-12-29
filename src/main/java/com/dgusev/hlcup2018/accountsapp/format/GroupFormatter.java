package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.Group;
import io.netty.buffer.ByteBuf;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class GroupFormatter {

    private static final byte[] COUNT = "{\"count\":".getBytes();
    private static final byte[] NULL = "null".getBytes();

    public void format(Group group, List<String> keys, ByteBuf responseBuf) {
        responseBuf.writeBytes(COUNT);
        encodeLong(group.count, responseBuf);
        for (int i = 0; i < group.values.size() && i < keys.size(); i++) {
                    if (group.values.get(i) != null) {
                        writeField(responseBuf, false, keys.get(i));
                        writeStringValue(responseBuf, group.values.get(i));
                    }
        }
        responseBuf.writeByte('}');
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
}
