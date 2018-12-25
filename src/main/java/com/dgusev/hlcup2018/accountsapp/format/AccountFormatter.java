package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import io.netty.buffer.ByteBuf;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class AccountFormatter {

    private static final byte[] START = "{\"start\":".getBytes();
    private static final byte[] FINISH = ",\"finish\":".getBytes();

    public void format(AccountDTO accountDTO, List<String> fields, ByteBuf responseBuf) {
        responseBuf.writeByte('{');
        boolean first = true;
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (field.equals("id")) {
                writeField(responseBuf, first, field);
                encodeLong(accountDTO.id, responseBuf);
                first = false;
            } else if (field.equals("email")) {
                writeField(responseBuf, first, field);
                writeStringValue(responseBuf, accountDTO.email);
                first = false;
            } else if (field.equals("fname")) {
                if (accountDTO.fname != null) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, accountDTO.fname);
                    first = false;
                }
            } else if (field.equals("sname")) {
                if (accountDTO.sname != null) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, accountDTO.sname);
                    first = false;
                }
            } else if (field.equals("phone")) {
                 if (accountDTO.phone != null) {
                     writeField(responseBuf, first, field);
                     writeStringValue(responseBuf, accountDTO.phone);
                     first = false;
                 }
            } else if (field.equals("sex")) {
                writeField(responseBuf, first, field);
                writeStringValue(responseBuf, accountDTO.sex);
                first = false;
            } else if (field.equals("birth")) {
                writeField(responseBuf, first, field);
                encodeLong(accountDTO.birth, responseBuf);
                first = false;
            } else if (field.equals("country")) {
                if (accountDTO.country != null) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, accountDTO.country);
                    first = false;
                }
            } else if (field.equals("city")) {
                if (accountDTO.city != null) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, accountDTO.city);
                    first = false;
                }
            } else if (field.equals("joined")) {
                writeField(responseBuf, first, field);
                encodeLong(accountDTO.joined, responseBuf);
                first = false;
            } else if (field.equals("status")) {
                writeField(responseBuf, first, field);
                writeStringValue(responseBuf, accountDTO.status);
                first = false;
            } else if (field.equals("premium")) {
                if (accountDTO.premiumStart != 0) {
                    writeField(responseBuf, first, field);
                    responseBuf.writeBytes(START);
                    encodeLong(accountDTO.premiumStart, responseBuf);
                    responseBuf.writeBytes(FINISH);
                    encodeLong(accountDTO.premiumFinish, responseBuf);
                    responseBuf.writeByte('}');
                    first = false;
                }
            }
        }
        responseBuf.writeByte('}');
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

    public void formatRecommend(AccountDTO accountDTO, ByteBuf responseBuf) {
        StringBuilder stringBuilder = new StringBuilder("{\"id\":");
        stringBuilder.append(accountDTO.id).append(",\"email\":\"").append(accountDTO.email).append("\",\"status\":\"").append(accountDTO.status).append("\",\"birth\":").append(accountDTO.birth);
        if (accountDTO.fname != null) {
            stringBuilder.append(",\"fname\":\"").append(accountDTO.fname).append("\"");
        }
        if (accountDTO.sname != null) {
            stringBuilder.append(",\"sname\":\"").append(accountDTO.sname).append("\"");
        }
        if (accountDTO.premiumStart != 0) {
            stringBuilder.append(",");
            stringBuilder.append("\"").append("premium").append("\":");
            stringBuilder.append("{\"start\":");
            stringBuilder.append(accountDTO.premiumStart);
            stringBuilder.append(",\"finish\":");
            stringBuilder.append(accountDTO.premiumFinish);
            stringBuilder.append("}");
        }
        stringBuilder.append("}");
        responseBuf.writeBytes(stringBuilder.toString().getBytes());
    }

    public void formatSuggest(AccountDTO accountDTO, ByteBuf responseBuf) {
        StringBuilder stringBuilder = new StringBuilder("{\"id\":");
        stringBuilder.append(accountDTO.id).append(",\"email\":\"").append(accountDTO.email).append("\",\"status\":\"").append(accountDTO.status).append("\"");
        if (accountDTO.fname != null) {
            stringBuilder.append(",\"fname\":\"").append(accountDTO.fname).append("\"");
        }
        if (accountDTO.sname != null) {
            stringBuilder.append(",\"sname\":\"").append(accountDTO.sname).append("\"");
        }
        stringBuilder.append("}");
        responseBuf.writeBytes(stringBuilder.toString().getBytes());
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

}
