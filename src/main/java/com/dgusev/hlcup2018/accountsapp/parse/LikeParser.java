package com.dgusev.hlcup2018.accountsapp.parse;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import com.dgusev.hlcup2018.accountsapp.model.LikeRequest;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class LikeParser {

    private static final BadRequest BAD_REQUEST = new BadRequest();

    private static TLongObjectMap<String> parametersMap = new TLongObjectHashMap<>();

    static {
        List<String> values = Arrays.asList("likee",
                "ts",
                "liker");
        for (String value:  values) {
            long hash = 0;
            for (int i = 0; i < value.length(); i++) {
                hash = 31* hash + (byte)value.charAt(i);
            }
            parametersMap.put(hash, value);
        }

    }

    public List<LikeRequest> parse(byte[] array, int length) {
        if (length < 2) {
            throw new BadRequest();
        }
        List<LikeRequest> requests =  new ArrayList<>();
        int currentIndex = indexOf(array, 0, length, '[');
        while (true) {
            if (indexOf(array, currentIndex, length,'{') == -1) {
                break;
            }
            currentIndex = indexOf(array, currentIndex, length,'{');
            LikeRequest likeRequest = new LikeRequest();
            while (true) {
                if (array[currentIndex] == '}') {
                    break;
                }
                int fromField = indexOf(array, currentIndex, length,'"');
                int toField = indexOf(array, fromField + 1, length,'"');
                long hash = calculateHash(array, fromField + 1, toField - fromField - 1 );
                String subField = parametersMap.get(hash);
                if (subField == null) {
                    throw BAD_REQUEST;
                }
                if (subField.equals("likee")) {
                    int nextColon = indexOf(array, toField + 1, length,':');
                    int nextComma = indexOf(array, nextColon + 1, length,',');
                    int nextClose = indexOf(array, nextColon + 1, length,'}');
                    if (nextComma == -1) {
                        currentIndex = indexOf(array, nextColon + 1, length,'}');
                    } else {
                        if (nextClose < nextComma) {
                            currentIndex = nextClose;
                        } else {
                            currentIndex = nextComma;
                        }
                    }
                    int endIndex = currentIndex;
                    nextColon++;
                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                        nextColon++;
                    }
                    endIndex--;
                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                        endIndex--;
                    }
                    likeRequest.likee = decodeInt(array, nextColon, endIndex + 1 - nextColon);
                } else if (subField.equals("ts")) {
                    int nextColon = indexOf(array, toField + 1, length,':');
                    int nextComma = indexOf(array, nextColon + 1, length,',');
                    int nextClose = indexOf(array, nextColon + 1, length,'}');
                    if (nextComma == -1) {
                        currentIndex = indexOf(array, nextColon + 1, length,'}');
                    } else {
                        if (nextClose < nextComma) {
                            currentIndex = nextClose;
                        } else {
                            currentIndex = nextComma;
                        }
                    }
                    int endIndex = currentIndex;
                    nextColon++;
                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                        nextColon++;
                    }
                    endIndex--;
                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                        endIndex--;
                    }
                    likeRequest.ts = decodeInt(array, nextColon, endIndex + 1 - nextColon);
                } else if (subField.equals("liker")) {
                    int nextColon = indexOf(array, toField + 1, length,':');
                    int nextComma = indexOf(array, nextColon + 1, length,',');
                    int nextClose = indexOf(array, nextColon + 1, length,'}');
                    if (nextComma == -1) {
                        currentIndex = indexOf(array, nextColon + 1, length,'}');
                    } else {
                        if (nextClose < nextComma) {
                            currentIndex = nextClose;
                        } else {
                            currentIndex = nextComma;
                        }
                    }
                    int endIndex = currentIndex;
                    nextColon++;
                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                        nextColon++;
                    }
                    endIndex--;
                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                        endIndex--;
                    }
                    likeRequest.liker = decodeInt(array, nextColon, endIndex + 1 - nextColon);
                } else {
                    throw new BadRequest();
                }
            }
            requests.add(likeRequest);
        }
        return requests;
    }

    private int indexOf(byte[] array, int from, int to, char ch) {
        for (int i = from; i < to; i++) {
            if (array[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    private static final int POW10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    private int decodeInt(byte[] buf, int from, int length) {
        if (length > 10) {
            throw BAD_REQUEST;
        }
        int result = 0;
        for (int i = from; i < from + length; i++) {
            int value = buf[i] - 48;
            if (value <0 || value > 9) {
                throw BAD_REQUEST;
            }
            result+=POW10[length - (i - from) - 1]* value;
        }
        return result;
    }

    private long calculateHash(byte[] array, int from, int length) {
        long result = 0;
        for (int i = from; i < from + length; i++) {
            result =31* result + array[i];
        }
        return result;
    }
}
