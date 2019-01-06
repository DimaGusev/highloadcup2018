package com.dgusev.hlcup2018.accountsapp.parse;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.*;

public class QueryParser {

    private static TLongObjectMap<String> parametersMap = new TLongObjectHashMap<>();
    private static TLongObjectMap<String> valuesMap = new TLongObjectHashMap<>();

    private static final BadRequest BAD_REQUEST = new BadRequest();

    static {
        List<String> values = Arrays.asList("query_id",
                "limit",
                "sex_eq",
                "email_domain",
                "email_lt",
                "email_gt",
                "status_eq",
                "status_neq",
                "fname_eq",
                "fname_any",
                "fname_null",
                "sname_eq",
                "sname_starts",
                "sname_null",
                "phone_code",
                "phone_null",
                "country_eq",
                "country_null",
                "city_eq",
                "city_any",
                "city_null",
                "birth_lt",
                "birth_gt",
                "birth_year",
                "interests_contains",
                "interests_any",
                "likes_contains",
                "premium_now",
                "premium_null",
                "keys",
                "order",
                "sex",
                "email",
                "status",
                "fname",
                "sname",
                "phone",
                "country",
                "city",
                "birth",
                "interests",
                "likes",
                "joined");
        for (String value:  values) {
            long hash = 0;
            for (int i = 0; i < value.length(); i++) {
                hash = 31* hash + value.charAt(i);
            }
            parametersMap.put(hash, value);
        }

        for (int i = -1; i <=50; i++) {
            String value = Integer.toString(i);
            long hash = 0;
            for (int j = 0; j < value.length(); j++) {
                hash = 31* hash + value.charAt(j);
            }
            valuesMap.put(hash, value);
        }

        for (int i = 1950; i <2020; i++) {
            String value = Integer.toString(i);
            long hash = 0;
            for (int j = 0; j < value.length(); j++) {
                hash = 31* hash + value.charAt(j);
            }
            valuesMap.put(hash, value);
        }
        List<String> values2 = Arrays.asList("m",
                "f",
                "status",
                "sex",
                "interests",
                "city",
                "country");
        for (String value:  values2) {
            long hash = 0;
            for (int i = 0; i < value.length(); i++) {
                hash = 31* hash + value.charAt(i);
            }
            valuesMap.put(hash, value);
        }



    }

    private static final ThreadLocal<Map<String, String>> parseMap = new ThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String, String> initialValue() {
            return new HashMap<>(500);
        }
    };

    private static final ThreadLocal<byte[]> tmpByteBuffer = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[500];
        }
    };


    public static Map<String, String> parse(String query, int start) {
        Map<String, String> result = parseMap.get();
        result.clear();
        int index = start;
        while (index != query.length()) {
            int from = index;
            int eq = query.indexOf('=', index);
            int next = query.indexOf('&', eq + 1);
            if (next == -1) {
                index = query.length();
                next = index;
            } else {
                index = next + 1;
            }

            result.put(getField(query, from, eq), decode(query,eq + 1, next));
        }
        return result;
    }

    private static String getField(String query, int from, int to) {
        long hash = 0;
        for (int i = from; i < to; i++) {
            hash = 31* hash + query.charAt(i);
        }
        String result = parametersMap.get(hash);
        if (result != null) {
            return result;
        } else {
            throw BAD_REQUEST;
        }
    }

    private static String decode(String parameter, int from, int to) {
        boolean decodingRequired = false;
        for (int i = from; i < to; i++) {
            char ch = parameter.charAt(i);
            if (ch == '%' || ch == '+') {
                decodingRequired = true;
                break;
            }
        }
        if (decodingRequired) {
            return decodePN(parameter, from, to);
        } else  {
            return parameter.substring(from, to);
        }
    }

    private static String decodePN(String parameter, int from, int to) {
        int count = 0;
        int index = from;
        while (index != to) {
            if (parameter.charAt(index) == '%') {
                index+=3;
                count++;
            } else {
                index++;
                count++;
            }
        }
        byte[] arr = new byte[count];
        index = from;
        int i = 0;
        while (index != to) {
            if (parameter.charAt(index) == '%') {
                arr[i++] = (byte) (covert(parameter.charAt(index + 1)) * 16 + covert(parameter.charAt(index + 2)));
                index+=3;
            } else if (parameter.charAt(index) == '+') {
                arr[i++] = ' ';
                index++;
            } else {
                arr[i++] = (byte)parameter.charAt(index);
                index++;
            }
        }
        return new String(arr);
    }

    private static int covert(char ch) {
        switch (ch) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'A':
                return 10;
            case 'B':
                return 11;
            case 'C':
                return 12;
            case 'D':
                return 13;
            case 'E':
                return 14;
            case 'F':
                return 15;
        }
        return 0;
    }


    public static Map<String, String> parse(byte[] query, int start, int end) {
        Map<String, String> result = parseMap.get();
        byte[] tmp = tmpByteBuffer.get();
        result.clear();
        if (start >= end) {
            return result;
        }
        int index = start;
        while (index != end) {
            int from = index;
            int eq = indexOf(query,index, end, '=');
            int next = indexOf(query,eq + 1, end, '&');
            if (next == -1) {
                index = end;
                next = index;
            } else {
                index = next + 1;
            }
            String field = getField(query, from, eq);
            if (!field.equals("query_id")) {
                result.put(field, decode(query, eq + 1, next, tmp));
            }
        }
        return result;
    }

    private static int indexOf(byte[] arr, int from, int to, char val) {
        for (int i = from; i < to; i++) {
            if (arr[i] == (byte) val) {
                return i;
            }
        }
        return -1;
    }

    private static String getField(byte[] query, int from, int to) {
        long hash = 0;
        for (int i = from; i < to; i++) {
            hash = 31* hash + query[i];
        }
        String result = parametersMap.get(hash);
        if (result != null) {
            return result;
        } else {
            throw BAD_REQUEST;
        }
    }

    private static String decode(byte[] parameter, int from, int to, byte[] tmp) {
        boolean decodingRequired = false;
        long hash = 0;
        for (int i = from; i < to; i++) {
            byte ch = parameter[i];
            if (ch == '%' || ch == '+') {
                decodingRequired = true;
                break;
            }
            hash = 31* hash + ch;
        }
        if (decodingRequired) {
            return decodePN(parameter, from, to, tmp);
        } else  {
            String value = valuesMap.get(hash);
            if (value != null) {
                return value;
            } else {
                return new String(parameter, from, to - from);
            }
        }
    }

    private static String decodePN(byte[] parameter, int from, int to, byte[] tmp) {
        int count = 0;
        int index = from;
        while (index != to) {
            if (parameter[index] == '%') {
                index+=3;
                count++;
            } else {
                index++;
                count++;
            }
        }
        index = from;
        int i = 0;
        while (index != to) {
            if (parameter[index] == '%') {
                tmp[i++] = (byte) (covert((char)parameter[index + 1]) * 16 + covert((char)parameter[index + 2]));
                index+=3;
            } else if (parameter[index] == '+') {
                tmp[i++] = ' ';
                index++;
            } else {
                tmp[i++] = (byte)parameter[index];
                index++;
            }
        }
        return new String(tmp, 0, count);
    }

}
