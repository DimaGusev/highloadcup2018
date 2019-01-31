package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
import gnu.trove.impl.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AccountFormatter {

    private static final byte[] START = "{\"start\":".getBytes();
    private static final byte[] FINISH = ",\"finish\":".getBytes();

    private static final byte[] ID = "id".getBytes();
    private static final byte[] EMAIL = "email".getBytes();
    private static final byte[] FNAME = "fname".getBytes();
    private static final byte[] SNAME = "sname".getBytes();
    private static final byte[] PHONE = "phone".getBytes();
    private static final byte[] SEX = "sex".getBytes();
    private static final byte[] BIRTH = "birth".getBytes();
    private static final byte[] COUNTRY = "country".getBytes();
    private static final byte[] CITY = "city".getBytes();
    private static final byte[] JOINED = "joined".getBytes();
    private static final byte[] STATUS = "status".getBytes();
    private static final byte[] PREMIUM = "premium".getBytes();
    private static final byte[] F_START = "start".getBytes();
    private static final byte[] F_FINISH = "finish".getBytes();
    private static final byte[] STATUS0 = "свободны".getBytes();
    private static final byte[] STATUS1 = "всё сложно".getBytes();
    private static final byte[] STATUS2 = "заняты".getBytes();

    @Autowired
    private Dictionary dictionary;

    public int format(Account account, int fieldsMap, byte[] arr, int initialPosition) {
        int index = initialPosition;
        arr[index++] = '{';
        index = writeField(arr, index, true, ID);
        index = encodeLong(arr, index, account.id);
        index = writeField(arr, index, false, EMAIL);
        index = writeStringValue(arr, index, account.email);
        if ((fieldsMap & 1) != 0) {
            if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                index = writeField(arr, index, false, FNAME);
                index = writeStringValue(arr, index, dictionary.getFnameBytes(account.fname));
            }
        }
        if ((fieldsMap & 2) != 0) {
            if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                index = writeField(arr, index, false, SNAME);
                index = writeStringValue(arr, index, dictionary.getSnameBytes(account.sname));
            }
        }
        if ((fieldsMap & 4) != 0) {
            if (account.phone != null) {
                index = writeField(arr, index, false, PHONE);
                index = writeStringValue(arr, index, account.phone);
            }
        }

        if ((fieldsMap & 8) != 0) {
            index = writeField(arr, index, false, SEX);
            index = writeSex(arr, index, account.sex);
        }

        if ((fieldsMap & 16) != 0) {
            index = writeField(arr, index, false, BIRTH);
            index = encodeLong(arr, index, account.birth);
        }

        if ((fieldsMap & 32) != 0) {
            if (account.country != Constants.DEFAULT_BYTE_NO_ENTRY_VALUE) {
                index = writeField(arr, index, false, COUNTRY);
                index = writeStringValue(arr, index, dictionary.getCountryBytes(account.country));
            }
        }
        if ((fieldsMap & 64) != 0) {
            if (account.city != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
                index = writeField(arr, index, false, CITY);
                index = writeStringValue(arr, index, dictionary.getCityBytes(account.city));
            }
        }

        if ((fieldsMap & 128) != 0) {
            index = writeField(arr, index, false, STATUS);
            index = writeStatus(arr, index, account.status);
        }

        if ((fieldsMap & 256) != 0) {
            if (account.premiumStart != 0) {
                index = writeField(arr, index, false, PREMIUM);
                System.arraycopy(START, 0, arr, index, START.length);
                index+=START.length;
                index = encodeLong(arr, index, account.premiumStart);
                System.arraycopy(FINISH, 0, arr, index, FINISH.length);
                index+=FINISH.length;
                index = encodeLong(arr, index, account.premiumFinish);
                arr[index++] = '}';
            }
        }

        arr[index++] = '}';
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

    private int writeStringValue(byte[] arr, int index, byte[] value) {
        arr[index++] = '\"';
        System.arraycopy(value, 0, arr, index, value.length);
        index+=value.length;
        arr[index++] = '\"';
        return index;
    }

    public int formatRecommend(Account account, byte[] arr, int startIndex) {
        int index = startIndex;
        arr[index++] = '{';
        index = writeField(arr, index, true, ID);
        index = encodeLong(arr, index, account.id);
        index = writeField(arr, index,false, EMAIL);
        index = writeStringValue(arr, index, account.email);
        index = writeField(arr, index, false, STATUS);
        index = writeStatus(arr, index, account.status);
        index = writeField(arr, index, false, BIRTH);
        index = encodeLong(arr, index, account.birth);
        if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            index = writeField(arr, index, false, FNAME);
            index = writeStringValue(arr, index, dictionary.getFnameBytes(account.fname));
        }
        if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            index = writeField(arr, index, false, SNAME);
            index = writeStringValue(arr, index, dictionary.getSnameBytes(account.sname));
        }
        if (account.premiumStart != 0) {
            index = writeField(arr, index, false, PREMIUM);
            arr[index++] = '{';
            index = writeField(arr, index, true, F_START);
            index = encodeLong(arr, index, account.premiumStart);
            index = writeField(arr, index, false, F_FINISH);
            index = encodeLong(arr, index, account.premiumFinish);
            arr[index++] = '}';
        }
        arr[index++] = '}';
        return index;
    }

    public int formatSuggest(Account account, byte[] arr, int startIndex) {
        int index = startIndex;
        arr[index++] = '{';
        index = writeField(arr, index, true, ID);
        index = encodeLong(arr, index, account.id);
        index = writeField(arr, index, false, EMAIL);
        index = writeStringValue(arr, index, account.email);
        index = writeField(arr, index, false, STATUS);
        index = writeStatus(arr, index, account.status);
        if (account.fname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            index = writeField(arr, index, false, FNAME);
            index = writeStringValue(arr, index, dictionary.getFnameBytes(account.fname));
        }
        if (account.sname != Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
            index = writeField(arr, index, false, SNAME);
            index = writeStringValue(arr, index, dictionary.getSnameBytes(account.sname));
        }
        arr[index++] = '}';
        return index;
    }

    private static final int POW10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};


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
