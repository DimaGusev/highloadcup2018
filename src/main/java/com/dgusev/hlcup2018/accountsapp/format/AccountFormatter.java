package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.service.ConvertorUtills;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
import gnu.trove.impl.Constants;
import io.netty.buffer.ByteBuf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class AccountFormatter {

    private static final byte[] START = "{\"start\":".getBytes();
    private static final byte[] FINISH = ",\"finish\":".getBytes();

    @Autowired
    private Dictionary dictionary;

    public void format(Account account, List<String> fields, ByteBuf responseBuf, byte[] arr) {
        int index = 0;
        arr[index++] = '{';
        boolean first = true;
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (field.equals("id")) {
                index = writeField(arr, index, first, field);
                index = encodeLong(arr, index, account.id, responseBuf);
                first = false;
            } else if (field.equals("email")) {
                index = writeField(arr, index, first, field);
                index = writeStringValue(arr, index, account.email);
                first = false;
            } else if (field.equals("fname")) {
                if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                    index = writeField(arr, index, first, field);
                    index = writeStringValue(arr, index, dictionary.getFname(account.fname));
                    first = false;
                }
            } else if (field.equals("sname")) {
                if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                    index = writeField(arr, index, first, field);
                    index = writeStringValue(arr, index, dictionary.getSname(account.sname));
                    first = false;
                }
            } else if (field.equals("phone")) {
                 if (account.phone != null) {
                     index = writeField(arr, index, first, field);
                     index = writeStringValue(arr, index, account.phone);
                     first = false;
                 }
            } else if (field.equals("sex")) {
                index = writeField(arr, index, first, field);
                index = writeStringValue(arr, index, convertSex(account.sex));
                first = false;
            } else if (field.equals("birth")) {
                index = writeField(arr, index, first, field);
                index = encodeLong(arr, index, account.birth, responseBuf);
                first = false;
            } else if (field.equals("country")) {
                if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                    index = writeField(arr, index, first, field);
                    index = writeStringValue(arr, index, dictionary.getCountry(account.country));
                    first = false;
                }
            } else if (field.equals("city")) {
                if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                    index = writeField(arr, index, first, field);
                    index = writeStringValue(arr, index, dictionary.getCity(account.city));
                    first = false;
                }
            } else if (field.equals("joined")) {
                index = writeField(arr, index, first, field);
                index = encodeLong(arr, index, account.joined, responseBuf);
                first = false;
            } else if (field.equals("status")) {
                index = writeField(arr, index, first, field);
                index = writeStringValue(arr, index, ConvertorUtills.convertStatusNumber(account.status));
                first = false;
            } else if (field.equals("premium")) {
                if (account.premiumStart != 0) {
                    index = writeField(arr, index, first, field);
                    System.arraycopy(START, 0, arr, index, START.length);
                    index+=START.length;
                    index = encodeLong(arr, index, account.premiumStart, responseBuf);
                    System.arraycopy(FINISH, 0, arr, index, FINISH.length);
                    index+=FINISH.length;
                    index = encodeLong(arr, index, account.premiumFinish, responseBuf);
                    arr[index++] = '}';
                    first = false;
                }
            }
        }
        arr[index++] = '}';
        responseBuf.writeBytes(arr, 0, index);
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

    public void formatRecommend(Account account, ByteBuf responseBuf) {
        responseBuf.writeByte('{');
        writeField(responseBuf, true, "id");
        encodeLong(account.id, responseBuf);
        writeField(responseBuf, false, "email");
        writeStringValue(responseBuf, account.email);
        writeField(responseBuf, false, "status");
        writeStringValue(responseBuf, ConvertorUtills.convertStatusNumber(account.status));
        writeField(responseBuf, false, "birth");
        encodeLong(account.birth, responseBuf);
        if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            writeField(responseBuf, false, "fname");
            writeStringValue(responseBuf, dictionary.getFname(account.fname));
        }
        if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            writeField(responseBuf, false, "sname");
            writeStringValue(responseBuf, dictionary.getSname(account.sname));
        }
        if (account.premiumStart != 0) {
            writeField(responseBuf, false, "premium");
            responseBuf.writeByte('{');
            writeField(responseBuf, true, "start");
            encodeLong(account.premiumStart, responseBuf);
            writeField(responseBuf, false, "finish");
            encodeLong(account.premiumFinish, responseBuf);
            responseBuf.writeByte('}');
        }
        responseBuf.writeByte('}');
    }

    public void formatSuggest(Account account, ByteBuf responseBuf, byte[] arr) {
        int index = 0;
        arr[index++] = '{';
        index = writeField(arr, index, true, "id");
        index = encodeLong(arr, index, account.id, responseBuf);
        index = writeField(arr, index, false, "email");
        index = writeStringValue(arr, index, account.email);
        index = writeField(arr, index, false, "status");
        index = writeStringValue(arr, index, ConvertorUtills.convertStatusNumber(account.status));
        if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            index = writeField(arr, index, false, "fname");
            index = writeStringValue(arr, index, dictionary.getFname(account.fname));
        }
        if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            index = writeField(arr, index, false, "sname");
            index = writeStringValue(arr, index, dictionary.getSname(account.sname));
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

    private String convertSex(boolean sex) {
        if (sex) {
            return "m";
        } else {
            return "f";
        }
    }

}
