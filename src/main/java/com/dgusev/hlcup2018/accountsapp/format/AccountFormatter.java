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

    public void format(Account account, List<String> fields, ByteBuf responseBuf) {
        responseBuf.writeByte('{');
        boolean first = true;
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (field.equals("id")) {
                writeField(responseBuf, first, field);
                encodeLong(account.id, responseBuf);
                first = false;
            } else if (field.equals("email")) {
                writeField(responseBuf, first, field);
                writeStringValue(responseBuf, account.email);
                first = false;
            } else if (field.equals("fname")) {
                if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, dictionary.getFname(account.fname));
                    first = false;
                }
            } else if (field.equals("sname")) {
                if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, dictionary.getSname(account.sname));
                    first = false;
                }
            } else if (field.equals("phone")) {
                 if (account.phone != null) {
                     writeField(responseBuf, first, field);
                     writeStringValue(responseBuf, account.phone);
                     first = false;
                 }
            } else if (field.equals("sex")) {
                writeField(responseBuf, first, field);
                writeStringValue(responseBuf, convertSex(account.sex));
                first = false;
            } else if (field.equals("birth")) {
                writeField(responseBuf, first, field);
                encodeLong(account.birth, responseBuf);
                first = false;
            } else if (field.equals("country")) {
                if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, dictionary.getCountry(account.country));
                    first = false;
                }
            } else if (field.equals("city")) {
                if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                    writeField(responseBuf, first, field);
                    writeStringValue(responseBuf, dictionary.getCity(account.city));
                    first = false;
                }
            } else if (field.equals("joined")) {
                writeField(responseBuf, first, field);
                encodeLong(account.joined, responseBuf);
                first = false;
            } else if (field.equals("status")) {
                writeField(responseBuf, first, field);
                writeStringValue(responseBuf, ConvertorUtills.convertStatusNumber(account.status));
                first = false;
            } else if (field.equals("premium")) {
                if (account.premiumStart != 0) {
                    writeField(responseBuf, first, field);
                    responseBuf.writeBytes(START);
                    encodeLong(account.premiumStart, responseBuf);
                    responseBuf.writeBytes(FINISH);
                    encodeLong(account.premiumFinish, responseBuf);
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

    public void formatSuggest(Account account, ByteBuf responseBuf) {
        responseBuf.writeByte('{');
        writeField(responseBuf, true, "id");
        encodeLong(account.id, responseBuf);
        writeField(responseBuf, false, "email");
        writeStringValue(responseBuf, account.email);
        writeField(responseBuf, false, "status");
        writeStringValue(responseBuf, ConvertorUtills.convertStatusNumber(account.status));
        if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            writeField(responseBuf, false, "fname");
            writeStringValue(responseBuf, dictionary.getFname(account.fname));
        }
        if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            writeField(responseBuf, false, "sname");
            writeStringValue(responseBuf, dictionary.getSname(account.sname));
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

    private String convertSex(boolean sex) {
        if (sex) {
            return "m";
        } else {
            return "f";
        }
    }

}
