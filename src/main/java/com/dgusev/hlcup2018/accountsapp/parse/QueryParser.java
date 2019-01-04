package com.dgusev.hlcup2018.accountsapp.parse;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryParser {

    private static TLongObjectMap<String> parametersMap = new TLongObjectHashMap<>();

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

    }

    private static final ThreadLocal<Map<String, String>> parseMap = new ThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String, String> initialValue() {
            return new HashMap<>(500);
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

}
